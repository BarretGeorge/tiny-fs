use std::{path::PathBuf, time::Duration};

use rusqlite::{params, Connection, OptionalExtension, Transaction};

use crate::{
    meta::{
        BlobRecord, Bucket, CompleteMultipartCommit, MultipartPartRecord, MultipartUpload,
        ObjectMetadata, ObjectRecord, PutObjectCommit,
    },
    AppError, AppResult, StoredBlob,
};

#[derive(Clone, Debug)]
pub struct SqliteMetadataStore {
    db_path: PathBuf,
}

impl SqliteMetadataStore {
    pub fn new(db_path: PathBuf) -> Self {
        Self { db_path }
    }

    pub fn initialize(&self) -> AppResult<()> {
        let conn = self.open_connection()?;
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS buckets (
                name TEXT PRIMARY KEY,
                created_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS blobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT NOT NULL UNIQUE,
                size INTEGER NOT NULL,
                checksum TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS objects (
                bucket_name TEXT NOT NULL,
                object_key TEXT NOT NULL,
                blob_id INTEGER NOT NULL,
                size INTEGER NOT NULL,
                etag TEXT NOT NULL,
                content_type TEXT,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (bucket_name, object_key),
                FOREIGN KEY(bucket_name) REFERENCES buckets(name) ON DELETE CASCADE,
                FOREIGN KEY(blob_id) REFERENCES blobs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS multipart_uploads (
                upload_id TEXT PRIMARY KEY,
                bucket_name TEXT NOT NULL,
                object_key TEXT NOT NULL,
                content_type TEXT,
                created_at INTEGER NOT NULL,
                FOREIGN KEY(bucket_name) REFERENCES buckets(name) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS multipart_parts (
                upload_id TEXT NOT NULL,
                part_number INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                size INTEGER NOT NULL,
                checksum TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (upload_id, part_number),
                FOREIGN KEY(upload_id) REFERENCES multipart_uploads(upload_id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_objects_bucket_key
            ON objects (bucket_name, object_key);

            CREATE INDEX IF NOT EXISTS idx_multipart_parts_upload
            ON multipart_parts (upload_id, part_number);
            "#,
        )?;
        Ok(())
    }

    pub fn clear_multipart_uploads(&self) -> AppResult<()> {
        let conn = self.open_connection()?;
        conn.execute("DELETE FROM multipart_uploads", [])?;
        Ok(())
    }

    pub fn create_bucket(&self, name: &str) -> AppResult<Bucket> {
        let conn = self.open_connection()?;

        if Self::bucket_exists(&conn, name)? {
            return Err(AppError::BucketAlreadyExists);
        }

        let created_at = now_unix_timestamp();
        conn.execute(
            "INSERT INTO buckets (name, created_at) VALUES (?1, ?2)",
            params![name, created_at],
        )?;

        Ok(Bucket {
            name: name.to_string(),
            created_at,
        })
    }

    pub fn delete_bucket(&self, name: &str) -> AppResult<()> {
        let conn = self.open_connection()?;

        if !Self::bucket_exists(&conn, name)? {
            return Err(AppError::BucketNotFound);
        }

        let object_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM objects WHERE bucket_name = ?1",
            params![name],
            |row| row.get(0),
        )?;

        if object_count > 0 {
            return Err(AppError::BucketNotEmpty);
        }

        conn.execute("DELETE FROM buckets WHERE name = ?1", params![name])?;
        Ok(())
    }

    pub fn list_buckets(&self) -> AppResult<Vec<Bucket>> {
        let conn = self.open_connection()?;
        let mut statement =
            conn.prepare("SELECT name, created_at FROM buckets ORDER BY name ASC")?;
        let rows = statement.query_map([], |row| {
            Ok(Bucket {
                name: row.get(0)?,
                created_at: row.get(1)?,
            })
        })?;

        let mut buckets = Vec::new();
        for row in rows {
            buckets.push(row?);
        }

        Ok(buckets)
    }

    pub fn get_object(&self, bucket: &str, key: &str) -> AppResult<ObjectRecord> {
        let conn = self.open_connection()?;
        Self::fetch_object_record(&conn, bucket, key)?.ok_or(AppError::ObjectNotFound)
    }

    pub fn list_objects(&self, bucket: &str) -> AppResult<Vec<ObjectMetadata>> {
        let conn = self.open_connection()?;

        if !Self::bucket_exists(&conn, bucket)? {
            return Err(AppError::BucketNotFound);
        }

        let mut statement = conn.prepare(
            r#"
            SELECT bucket_name, object_key, size, etag, content_type, created_at
            FROM objects
            WHERE bucket_name = ?1
            ORDER BY object_key ASC
            "#,
        )?;
        let rows = statement.query_map(params![bucket], |row| {
            Ok(ObjectMetadata {
                bucket: row.get(0)?,
                key: row.get(1)?,
                size: i64_to_u64("object size", row.get::<_, i64>(2)?).map_err(to_sqlite_error)?,
                etag: row.get(3)?,
                content_type: row.get(4)?,
                created_at: row.get(5)?,
            })
        })?;

        let mut objects = Vec::new();
        for row in rows {
            objects.push(row?);
        }

        Ok(objects)
    }

    pub fn put_object(
        &self,
        bucket: &str,
        key: &str,
        blob: &StoredBlob,
        content_type: Option<&str>,
    ) -> AppResult<PutObjectCommit> {
        let mut conn = self.open_connection()?;
        let tx = conn.transaction()?;

        if !Self::bucket_exists_tx(&tx, bucket)? {
            return Err(AppError::BucketNotFound);
        }

        let commit = Self::upsert_object_tx(&tx, bucket, key, blob, content_type)?;
        tx.commit()?;
        Ok(commit)
    }

    pub fn delete_object(&self, bucket: &str, key: &str) -> AppResult<BlobRecord> {
        let mut conn = self.open_connection()?;
        let tx = conn.transaction()?;

        let record =
            Self::fetch_object_record_tx(&tx, bucket, key)?.ok_or(AppError::ObjectNotFound)?;

        tx.execute(
            "DELETE FROM objects WHERE bucket_name = ?1 AND object_key = ?2",
            params![bucket, key],
        )?;
        tx.execute("DELETE FROM blobs WHERE id = ?1", params![record.blob.id])?;
        tx.commit()?;

        Ok(record.blob)
    }

    pub fn create_multipart_upload(
        &self,
        upload_id: &str,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
    ) -> AppResult<MultipartUpload> {
        let conn = self.open_connection()?;

        if !Self::bucket_exists(&conn, bucket)? {
            return Err(AppError::BucketNotFound);
        }

        let created_at = now_unix_timestamp();
        conn.execute(
            r#"
            INSERT INTO multipart_uploads (
                upload_id,
                bucket_name,
                object_key,
                content_type,
                created_at
            )
            VALUES (?1, ?2, ?3, ?4, ?5)
            "#,
            params![upload_id, bucket, key, content_type, created_at],
        )?;

        Ok(MultipartUpload {
            upload_id: upload_id.to_string(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            content_type: content_type.map(str::to_owned),
            created_at,
        })
    }

    pub fn put_multipart_part(
        &self,
        upload_id: &str,
        bucket: &str,
        key: &str,
        part_number: u16,
        blob: &StoredBlob,
    ) -> AppResult<Option<MultipartPartRecord>> {
        let mut conn = self.open_connection()?;
        let tx = conn.transaction()?;

        let upload = Self::fetch_multipart_upload_tx(&tx, upload_id)?
            .ok_or(AppError::MultipartUploadNotFound)?;
        if upload.bucket != bucket || upload.key != key {
            return Err(AppError::MultipartUploadNotFound);
        }

        let previous = Self::fetch_multipart_part_tx(&tx, upload_id, part_number)?;
        let created_at = now_unix_timestamp();

        tx.execute(
            r#"
            INSERT INTO multipart_parts (
                upload_id,
                part_number,
                file_path,
                size,
                checksum,
                created_at
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(upload_id, part_number)
            DO UPDATE SET
                file_path = excluded.file_path,
                size = excluded.size,
                checksum = excluded.checksum,
                created_at = excluded.created_at
            "#,
            params![
                upload_id,
                i64::from(part_number),
                &blob.relative_path,
                u64_to_i64(blob.size, "multipart part size")?,
                &blob.checksum,
                created_at
            ],
        )?;

        tx.commit()?;
        Ok(previous)
    }

    pub fn complete_multipart_upload(
        &self,
        upload_id: &str,
        bucket: &str,
        key: &str,
        blob: &StoredBlob,
    ) -> AppResult<CompleteMultipartCommit> {
        let mut conn = self.open_connection()?;
        let tx = conn.transaction()?;

        let upload = Self::fetch_multipart_upload_tx(&tx, upload_id)?
            .ok_or(AppError::MultipartUploadNotFound)?;
        if upload.bucket != bucket || upload.key != key {
            return Err(AppError::MultipartUploadNotFound);
        }

        let parts = Self::list_multipart_parts_tx(&tx, upload_id)?;
        if parts.is_empty() {
            return Err(AppError::InvalidRequest(
                "multipart upload has no parts".to_string(),
            ));
        }

        let put = Self::upsert_object_tx(&tx, bucket, key, blob, upload.content_type.as_deref())?;
        let part_paths = parts.into_iter().map(|part| part.file_path).collect();

        tx.execute(
            "DELETE FROM multipart_uploads WHERE upload_id = ?1",
            params![upload_id],
        )?;
        tx.commit()?;

        Ok(CompleteMultipartCommit { put, part_paths })
    }

    pub fn list_multipart_parts(
        &self,
        upload_id: &str,
        bucket: &str,
        key: &str,
    ) -> AppResult<(MultipartUpload, Vec<MultipartPartRecord>)> {
        let mut conn = self.open_connection()?;
        let tx = conn.transaction()?;

        let upload = Self::fetch_multipart_upload_tx(&tx, upload_id)?
            .ok_or(AppError::MultipartUploadNotFound)?;
        if upload.bucket != bucket || upload.key != key {
            return Err(AppError::MultipartUploadNotFound);
        }

        let parts = Self::list_multipart_parts_tx(&tx, upload_id)?;
        tx.commit()?;

        Ok((upload, parts))
    }

    pub fn abort_multipart_upload(
        &self,
        upload_id: &str,
        bucket: &str,
        key: &str,
    ) -> AppResult<Vec<String>> {
        let mut conn = self.open_connection()?;
        let tx = conn.transaction()?;

        let upload = Self::fetch_multipart_upload_tx(&tx, upload_id)?
            .ok_or(AppError::MultipartUploadNotFound)?;
        if upload.bucket != bucket || upload.key != key {
            return Err(AppError::MultipartUploadNotFound);
        }

        let part_paths = Self::list_multipart_parts_tx(&tx, upload_id)?
            .into_iter()
            .map(|part| part.file_path)
            .collect();

        tx.execute(
            "DELETE FROM multipart_uploads WHERE upload_id = ?1",
            params![upload_id],
        )?;
        tx.commit()?;

        Ok(part_paths)
    }

    fn open_connection(&self) -> AppResult<Connection> {
        let conn = Connection::open(&self.db_path)?;
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.execute_batch(
            r#"
            PRAGMA foreign_keys = ON;
            PRAGMA journal_mode = WAL;
            "#,
        )?;
        Ok(conn)
    }

    fn upsert_object_tx(
        tx: &Transaction<'_>,
        bucket: &str,
        key: &str,
        blob: &StoredBlob,
        content_type: Option<&str>,
    ) -> AppResult<PutObjectCommit> {
        if !Self::bucket_exists_tx(tx, bucket)? {
            return Err(AppError::BucketNotFound);
        }

        let previous = Self::fetch_object_record_tx(tx, bucket, key)?.map(|record| record.blob);
        let created_at = now_unix_timestamp();

        tx.execute(
            r#"
            INSERT INTO blobs (file_path, size, checksum, created_at)
            VALUES (?1, ?2, ?3, ?4)
            "#,
            params![
                &blob.relative_path,
                u64_to_i64(blob.size, "blob size")?,
                &blob.checksum,
                created_at
            ],
        )?;
        let blob_id = tx.last_insert_rowid();

        tx.execute(
            r#"
            INSERT INTO objects (
                bucket_name,
                object_key,
                blob_id,
                size,
                etag,
                content_type,
                created_at
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(bucket_name, object_key)
            DO UPDATE SET
                blob_id = excluded.blob_id,
                size = excluded.size,
                etag = excluded.etag,
                content_type = excluded.content_type,
                created_at = excluded.created_at
            "#,
            params![
                bucket,
                key,
                blob_id,
                u64_to_i64(blob.size, "object size")?,
                &blob.checksum,
                content_type,
                created_at
            ],
        )?;

        if let Some(previous_blob) = &previous {
            tx.execute("DELETE FROM blobs WHERE id = ?1", params![previous_blob.id])?;
        }

        Ok(PutObjectCommit {
            metadata: ObjectMetadata {
                bucket: bucket.to_string(),
                key: key.to_string(),
                size: blob.size,
                etag: blob.checksum.clone(),
                content_type: content_type.map(str::to_owned),
                created_at,
            },
            previous_blob: previous,
        })
    }

    fn bucket_exists(conn: &Connection, name: &str) -> AppResult<bool> {
        let exists = conn
            .query_row(
                "SELECT 1 FROM buckets WHERE name = ?1 LIMIT 1",
                params![name],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        Ok(exists)
    }

    fn bucket_exists_tx(tx: &Transaction<'_>, name: &str) -> AppResult<bool> {
        let exists = tx
            .query_row(
                "SELECT 1 FROM buckets WHERE name = ?1 LIMIT 1",
                params![name],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        Ok(exists)
    }

    fn fetch_object_record(
        conn: &Connection,
        bucket: &str,
        key: &str,
    ) -> AppResult<Option<ObjectRecord>> {
        conn.query_row(
            r#"
            SELECT
                o.bucket_name,
                o.object_key,
                o.size,
                o.etag,
                o.content_type,
                o.created_at,
                b.id,
                b.file_path,
                b.size,
                b.checksum,
                b.created_at
            FROM objects o
            JOIN blobs b ON b.id = o.blob_id
            WHERE o.bucket_name = ?1 AND o.object_key = ?2
            "#,
            params![bucket, key],
            read_object_record,
        )
        .optional()
        .map_err(Into::into)
    }

    fn fetch_object_record_tx(
        tx: &Transaction<'_>,
        bucket: &str,
        key: &str,
    ) -> AppResult<Option<ObjectRecord>> {
        tx.query_row(
            r#"
            SELECT
                o.bucket_name,
                o.object_key,
                o.size,
                o.etag,
                o.content_type,
                o.created_at,
                b.id,
                b.file_path,
                b.size,
                b.checksum,
                b.created_at
            FROM objects o
            JOIN blobs b ON b.id = o.blob_id
            WHERE o.bucket_name = ?1 AND o.object_key = ?2
            "#,
            params![bucket, key],
            read_object_record,
        )
        .optional()
        .map_err(Into::into)
    }

    fn fetch_multipart_upload_tx(
        tx: &Transaction<'_>,
        upload_id: &str,
    ) -> AppResult<Option<MultipartUpload>> {
        tx.query_row(
            r#"
            SELECT upload_id, bucket_name, object_key, content_type, created_at
            FROM multipart_uploads
            WHERE upload_id = ?1
            "#,
            params![upload_id],
            |row| {
                Ok(MultipartUpload {
                    upload_id: row.get(0)?,
                    bucket: row.get(1)?,
                    key: row.get(2)?,
                    content_type: row.get(3)?,
                    created_at: row.get(4)?,
                })
            },
        )
        .optional()
        .map_err(Into::into)
    }

    fn fetch_multipart_part_tx(
        tx: &Transaction<'_>,
        upload_id: &str,
        part_number: u16,
    ) -> AppResult<Option<MultipartPartRecord>> {
        tx.query_row(
            r#"
            SELECT upload_id, part_number, file_path, size, checksum, created_at
            FROM multipart_parts
            WHERE upload_id = ?1 AND part_number = ?2
            "#,
            params![upload_id, i64::from(part_number)],
            read_multipart_part_record,
        )
        .optional()
        .map_err(Into::into)
    }

    fn list_multipart_parts_tx(
        tx: &Transaction<'_>,
        upload_id: &str,
    ) -> AppResult<Vec<MultipartPartRecord>> {
        let mut statement = tx.prepare(
            r#"
            SELECT upload_id, part_number, file_path, size, checksum, created_at
            FROM multipart_parts
            WHERE upload_id = ?1
            ORDER BY part_number ASC
            "#,
        )?;
        let rows = statement.query_map(params![upload_id], read_multipart_part_record)?;

        let mut parts = Vec::new();
        for row in rows {
            parts.push(row?);
        }

        Ok(parts)
    }
}

fn read_object_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<ObjectRecord> {
    Ok(ObjectRecord {
        metadata: ObjectMetadata {
            bucket: row.get(0)?,
            key: row.get(1)?,
            size: i64_to_u64("object size", row.get::<_, i64>(2)?).map_err(to_sqlite_error)?,
            etag: row.get(3)?,
            content_type: row.get(4)?,
            created_at: row.get(5)?,
        },
        blob: BlobRecord {
            id: row.get(6)?,
            file_path: row.get(7)?,
            size: i64_to_u64("blob size", row.get::<_, i64>(8)?).map_err(to_sqlite_error)?,
            checksum: row.get(9)?,
            created_at: row.get(10)?,
        },
    })
}

fn read_multipart_part_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<MultipartPartRecord> {
    Ok(MultipartPartRecord {
        upload_id: row.get(0)?,
        part_number: i64_to_u16("multipart part number", row.get::<_, i64>(1)?)
            .map_err(to_sqlite_error)?,
        file_path: row.get(2)?,
        size: i64_to_u64("multipart part size", row.get::<_, i64>(3)?).map_err(to_sqlite_error)?,
        checksum: row.get(4)?,
        created_at: row.get(5)?,
    })
}

fn now_unix_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time should move forward")
        .as_secs() as i64
}

fn u64_to_i64(value: u64, field: &str) -> AppResult<i64> {
    i64::try_from(value)
        .map_err(|_| AppError::StorageInconsistent(format!("{field} exceeds SQLite integer range")))
}

fn i64_to_u64(field: &str, value: i64) -> AppResult<u64> {
    u64::try_from(value)
        .map_err(|_| AppError::StorageInconsistent(format!("{field} is negative in metadata")))
}

fn i64_to_u16(field: &str, value: i64) -> AppResult<u16> {
    u16::try_from(value).map_err(|_| {
        AppError::StorageInconsistent(format!("{field} exceeds u16 range in metadata"))
    })
}

fn to_sqlite_error(error: AppError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Integer, Box::new(error))
}
