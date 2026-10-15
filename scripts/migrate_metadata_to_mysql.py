"""Migrate ingestion history and image metadata; preserve Chroma, BM25 and images."""
import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sqlalchemy import select, func
from src.core.database import MetadataStore, ingestion_history, image_index


def migrate(source_root, backup_dir, target_url=None):
    source_root, backup_dir = Path(source_root), Path(backup_dir)
    backup_dir.mkdir(parents=True, exist_ok=True)
    records = {}
    for table in (ingestion_history, image_index):
        source = source_root / 'data' / 'db' / (table.name + '.db')
        if not source.is_file():
            raise FileNotFoundError(f'Missing source metadata database: {table.name}')
        # Immutable mode is only safe without pending WAL pages.
        if Path(str(source) + '-wal').exists():
            raise RuntimeError('Stop ingestion and checkpoint the SQLite WAL before migration')
        backup = backup_dir / source.name
        if backup.exists():
            raise FileExistsError('Use a new backup directory to preserve earlier snapshots')
        with sqlite3.connect(source.resolve().as_uri() + '?mode=ro&immutable=1', uri=True) as src:
            with sqlite3.connect(backup) as dst:
                src.backup(dst)
                if dst.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
                    raise RuntimeError('SQLite integrity check failed')
                dst.row_factory = sqlite3.Row
                records[table.name] = [dict(row) for row in dst.execute(f'SELECT * FROM {table.name}')]
        for row in records[table.name]:
            for column in table.columns:
                limit = getattr(column.type, 'length', None)
                value = row.get(column.name)
                if limit and isinstance(value, str) and len(value) > limit:
                    raise ValueError(f'{table.name}.{column.name} exceeds {limit} characters')
    store = MetadataStore(target_url)
    try:
        if store.engine.dialect.name != 'mysql':
            raise ValueError('Migration target must be MySQL')
        with store.engine.begin() as connection:
            for table in (ingestion_history, image_index):
                if connection.scalar(select(func.count()).select_from(table)):
                    raise ValueError('Target contains metadata; migration requires an empty target')
            for table in (ingestion_history, image_index):
                rows = records[table.name]
                if rows:
                    connection.execute(table.insert(), rows)
                copied = [dict(row) for row in connection.execute(select(table)).mappings()]
                key = next(iter(table.primary_key.columns)).name
                if sorted(rows, key=lambda row: row[key]) != sorted(copied, key=lambda row: row[key]):
                    raise RuntimeError(f'Exact content verification failed: {table.name}')
        return {name: len(rows) for name, rows in records.items()}
    finally:
        store.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--backup-dir', type=Path, required=True)
    args = parser.parse_args()
    try:
        for table, count in migrate(args.source_root, args.backup_dir).items():
            print(f'{table}: {count} rows migrated and verified')
    except Exception as exc:
        # Avoid exposing credentials and document contents in driver exceptions.
        print(f'Migration failed ({type(exc).__name__}); check configuration, source and target.', file=sys.stderr)
        sys.exit(1)
