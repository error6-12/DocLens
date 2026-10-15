from concurrent.futures import ThreadPoolExecutor
import pytest
from sqlalchemy import select
from sqlalchemy.schema import CreateTable
from sqlalchemy.dialects import mysql
from src.core.database import MetadataStore, metadata, ingestion_history
from src.libs.loader.database_integrity import DatabaseIntegrityChecker


def test_mysql_schema_compiles():
    for table in metadata.sorted_tables:
        assert 'CREATE TABLE' in str(CreateTable(table).compile(dialect=mysql.dialect()))


def test_retry_preserves_first_timestamp_and_collection(tmp_path):
    checker = DatabaseIntegrityChecker(str(tmp_path / 'retry.db'))
    try:
        checker.mark_success('a' * 64, '中文路径.pdf', '研发')
        first = checker.list_processed()[0]['processed_at']
        checker.mark_failed('a' * 64, '中文路径.pdf', '临时错误')
        assert not checker.should_skip('a' * 64)
        with checker.store.engine.connect() as connection:
            failed = connection.execute(select(ingestion_history)).mappings().one()
            assert failed['processed_at'] == first
            assert failed['collection'] == '研发'
        checker.mark_success('a' * 64, '中文路径.pdf', '研发')
        assert checker.list_processed('研发')[0]['processed_at'] == first
        assert checker.should_skip('a' * 64)
    finally:
        checker.close()


def test_concurrent_upsert_has_one_record(tmp_path):
    checker = DatabaseIntegrityChecker(str(tmp_path / 'concurrent.db'))
    try:
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _: checker.mark_success('b' * 64, '/a.pdf', 'test'), range(12)))
        assert len(checker.list_processed()) == 1
    finally:
        checker.close()


def test_failed_transaction_leaves_no_rows(tmp_path):
    store = MetadataStore(str(tmp_path / 'rollback.db'))
    try:
        with pytest.raises(RuntimeError):
            with store.engine.begin() as connection:
                connection.execute(ingestion_history.insert().values(
                    file_hash='c' * 64, file_path='/a.pdf', status='success',
                    processed_at='2026-10-14T00:00:00+00:00', updated_at='2026-10-14T00:00:00+00:00'))
                raise RuntimeError('rollback')
        with store.engine.connect() as connection:
            assert connection.execute(select(ingestion_history)).first() is None
    finally:
        store.close()
