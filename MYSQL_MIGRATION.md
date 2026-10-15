# DocLens MySQL metadata migration

The application stores ingestion history and image metadata in MySQL 8.0+
through SQLAlchemy and PyMySQL. Chroma continues to store vector data and chunk
metadata. BM25 index files and original image files keep their existing paths.
Chroma may internally use SQLite; this change replaces application-owned SQLite
metadata stores, rather than replacing Chroma's implementation.

Set DOCLENS_MYSQL_HOST, DOCLENS_MYSQL_PORT, DOCLENS_MYSQL_DATABASE,
DOCLENS_MYSQL_USER and DOCLENS_MYSQL_PASSWORD in the repo .env file.
DOCLENS_DATABASE_URL, if set, takes precedence. Generic DATABASE_URL and MYSQL_*
variables are ignored to avoid mixing the two projects.

Install the project dependencies, then stop document ingestion before migration:

```powershell
python scripts/migrate_metadata_to_mysql.py --backup-dir work/before-mysql
```

The target must be empty. The script preserves IDs, paths, statuses, error messages
and UTC ISO timestamps exactly, checks SQLite integrity and string lengths, and
verifies every row inside one MySQL transaction. SQLite source files are retained.
If pending SQLite WAL data exists, stop ingestion and checkpoint it before retrying.

Explicit SQLite paths remain available for isolated legacy tests.
DatabaseIntegrityChecker is the production implementation. MySQL atomic upserts
preserve the first processed_at timestamp; failed updates retain the collection.
The dashboard's existing clear-data action now clears the MySQL metadata tables.

Use Apply-MySQL.ps1 after verification to install the selected changes into the
original desktop project. It backs up edited files and preserves existing settings.
The script checks hashes first and refuses to overwrite files changed meanwhile.
It requires the local work/doclens/connection.env prepared during this migration;
that file contains credentials and is not intended for distribution.

Reference: https://docs.sqlalchemy.org/en/20/dialects/mysql.html#insert-on-duplicate-key-update-upsert
