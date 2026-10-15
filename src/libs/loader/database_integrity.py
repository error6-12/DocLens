"""SQLAlchemy implementation of ingestion history tracking."""
from datetime import datetime, timezone
from sqlalchemy import select
from src.core.database import MetadataStore, ingestion_history
from src.libs.loader.file_integrity import FileIntegrityChecker, SQLiteIntegrityChecker


class DatabaseIntegrityChecker(FileIntegrityChecker):
    # Hashing is independent of the storage backend.
    compute_sha256 = SQLiteIntegrityChecker.compute_sha256

    def __init__(self, database_url=None):
        self.store = MetadataStore(database_url, tables=[ingestion_history])

    def close(self):
        self.store.close()

    def should_skip(self, file_hash):
        with self.store.engine.connect() as connection:
            return connection.scalar(select(ingestion_history.c.status).where(
                ingestion_history.c.file_hash == file_hash)) == 'success'

    def mark_success(self, file_hash, file_path, collection=None):
        now = datetime.now(timezone.utc).isoformat()
        self.store.upsert(ingestion_history, dict(
            file_hash=file_hash, file_path=file_path, status='success',
            collection=collection, error_msg=None, processed_at=now, updated_at=now),
            preserve=('processed_at',))

    def mark_failed(self, file_hash, file_path, error_msg):
        now = datetime.now(timezone.utc).isoformat()
        self.store.upsert(ingestion_history, dict(
            file_hash=file_hash, file_path=file_path, status='failed',
            collection=None, error_msg=error_msg, processed_at=now, updated_at=now),
            preserve=('processed_at', 'collection'))

    def remove_record(self, file_hash):
        with self.store.engine.begin() as connection:
            return connection.execute(ingestion_history.delete().where(
                ingestion_history.c.file_hash == file_hash)).rowcount > 0

    def list_processed(self, collection=None):
        query = select(ingestion_history.c.file_hash, ingestion_history.c.file_path,
                       ingestion_history.c.collection, ingestion_history.c.processed_at,
                       ingestion_history.c.updated_at).where(ingestion_history.c.status == 'success')
        if collection is not None:
            query = query.where(ingestion_history.c.collection == collection)
        with self.store.engine.connect() as connection:
            return [dict(row) for row in connection.execute(
                query.order_by(ingestion_history.c.processed_at.asc())).mappings()]

    def clear(self):
        with self.store.engine.begin() as connection:
            connection.execute(ingestion_history.delete())
