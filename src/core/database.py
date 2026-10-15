"""Relational metadata storage; MySQL in the application, SQLite in isolated tests."""
import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import Column, Index, Integer, MetaData, String, Table, Text, create_engine
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import URL, make_url

metadata = MetaData()
ingestion_history = Table(
    'ingestion_history', metadata,
    Column('file_hash', String(64), primary_key=True),
    Column('file_path', Text, nullable=False),
    Column('status', String(16), nullable=False, index=True),
    Column('collection', String(191)),
    Column('error_msg', Text),
    # Preserve the original UTC ISO strings exactly, including timezone and microseconds.
    Column('processed_at', String(64), nullable=False),
    Column('updated_at', String(64), nullable=False),
    mysql_charset='utf8mb4', mysql_collate='utf8mb4_bin', mysql_engine='InnoDB',
)
image_index = Table(
    'image_index', metadata,
    Column('image_id', String(191), primary_key=True),
    Column('file_path', Text, nullable=False),
    Column('collection', String(191)),
    Column('doc_hash', String(64)),
    Column('page_num', Integer),
    Column('created_at', String(64), nullable=False),
    mysql_charset='utf8mb4', mysql_collate='utf8mb4_bin', mysql_engine='InnoDB',
)

Index('idx_collection', image_index.c.collection)
Index('idx_doc_hash', image_index.c.doc_hash)

def database_url():
    load_dotenv(Path(__file__).resolve().parents[2] / '.env')
    return os.getenv('DOCLENS_DATABASE_URL') or URL.create(
        'mysql+pymysql', username=os.getenv('DOCLENS_MYSQL_USER', 'doclens'),
        password=os.getenv('DOCLENS_MYSQL_PASSWORD', ''),
        host=os.getenv('DOCLENS_MYSQL_HOST', '127.0.0.1'),
        port=int(os.getenv('DOCLENS_MYSQL_PORT', '3306')),
        database=os.getenv('DOCLENS_MYSQL_DATABASE', 'doclens'),
        query={'charset': 'utf8mb4'},
    ).render_as_string(hide_password=False)


class MetadataStore:
    def __init__(self, db_path=None, tables=None):
        url = db_path if db_path is not None else database_url()
        if '://' not in str(url):
            path = Path(url).resolve()
            path.parent.mkdir(parents=True, exist_ok=True)
            url = URL.create('sqlite', database=str(path))
        backend = make_url(url).get_backend_name()
        if backend not in {'mysql', 'sqlite'}:
            raise ValueError('Metadata database must be MySQL or SQLite')
        options = {'pool_pre_ping': True}
        if backend == 'mysql':
            options.update(pool_size=5, max_overflow=10, pool_recycle=1800)
        self.engine = create_engine(url, **options)
        metadata.create_all(self.engine, tables=tables)
        if backend == 'sqlite':
            with self.engine.connect() as connection:
                connection.exec_driver_sql('PRAGMA journal_mode=WAL')

    def upsert(self, table, values, preserve=()):
        """Atomic upsert avoids races between read-before-write operations."""
        for column in table.columns:
            limit = getattr(column.type, 'length', None)
            value = values.get(column.name)
            if limit and isinstance(value, str) and len(value) > limit:
                raise ValueError(f'{table.name}.{column.name} exceeds {limit} characters')
        if self.engine.dialect.name == 'mysql':
            statement = mysql_insert(table).values(**values)
            updates = {k: statement.inserted[k] for k in values
                       if k not in preserve and not table.c[k].primary_key}
            statement = statement.on_duplicate_key_update(**updates)
        else:
            statement = sqlite_insert(table).values(**values)
            updates = {k: statement.excluded[k] for k in values
                       if k not in preserve and not table.c[k].primary_key}
            statement = statement.on_conflict_do_update(
                index_elements=list(table.primary_key.columns), set_=updates)
        with self.engine.begin() as connection:
            connection.execute(statement)

    def close(self):
        self.engine.dispose()
