# Rust MinIO-like FS Plan

## 1. Assumption

This plan treats the project as a **single-node, MinIO-style object storage system** built in Rust.
It is **not** a kernel-level POSIX filesystem like ext4, and it does **not** target distributed
erasure coding in the first version.

If the target later changes to a FUSE-mounted filesystem, the metadata and storage layers can be
reused, and only the API/access layer needs to be replaced.

## 2. MVP Scope

### Goals

- Buckets: create, delete, list
- Objects: put, get, head, delete
- Object listing with `prefix` and `delimiter`
- Streaming upload/download for large files
- Metadata durability and restart recovery
- Basic HTTP API compatible with a small S3-like subset

### Non-goals for v1

- Multi-node cluster
- Erasure coding
- IAM/RBAC completeness
- Replication
- Versioning
- Lifecycle rules
- Full S3 compatibility
- Encryption at rest

## 3. Recommended Architecture

Use a **metadata database + local blob store** design.

- Metadata DB: SQLite (`rusqlite`)
- Blob storage: local disk files managed by `tokio::fs`
- API layer: `axum`
- Async runtime: `tokio`
- Logging/metrics: `tracing`

This is the most pragmatic v1 because:

- metadata changes need transactions
- object files should support streaming IO
- crash recovery is easier than with pure file-path metadata
- later extensions like multipart upload or versioning are straightforward

## 4. Module Layout

Start with a **single crate** instead of a workspace. Split later only if boundaries become painful.

Suggested structure:

```text
src/
  main.rs
  config.rs
  error.rs
  api/
    mod.rs
    routes.rs
    handlers.rs
  service/
    mod.rs
    bucket.rs
    object.rs
  meta/
    mod.rs
    models.rs
    sqlite.rs
  storage/
    mod.rs
    blob_store.rs
    staging.rs
  http/
    mod.rs
    extractors.rs
    response.rs
  background/
    mod.rs
    gc.rs
tests/
  bucket_api.rs
  object_api.rs
  recovery.rs
```

## 5. Core Data Model

Recommended tables:

- `buckets`
  - `id`
  - `name`
  - `created_at`

- `objects`
  - `id`
  - `bucket_id`
  - `object_key`
  - `blob_id`
  - `size`
  - `etag`
  - `content_type`
  - `created_at`
  - `deleted_at`

- `blobs`
  - `id`
  - `file_path`
  - `size`
  - `checksum`
  - `created_at`

- `multipart_uploads` (phase 2)
- `multipart_parts` (phase 2)

Important decision:

- Keep object metadata in SQLite
- Keep actual bytes in opaque blob files on disk
- Do not encode business truth directly into directory names

## 6. Disk Layout

```text
data/
  meta.db
  blobs/
    aa/
      aa3f...blob
    b1/
      b1c2...blob
  staging/
    upload-uuid-1.part
    upload-uuid-2.part
```

Rules:

- New uploads first go to `staging/`
- After checksum/size validation, atomically move to `blobs/`
- Only after blob move succeeds, commit metadata transaction
- On restart, scan `staging/` and clean incomplete temp files

## 7. Request Flow

### PUT Object

1. Validate bucket/key
2. Stream request body to staging file
3. Compute checksum and size while streaming
4. Move file from staging to final blob path
5. Insert/update metadata in SQLite transaction
6. Return object metadata

### GET Object

1. Resolve bucket/key in metadata
2. Open blob file
3. Stream file back to client

### DELETE Object

1. Mark object deleted or remove metadata row
2. Decrement blob reference if shared in the future
3. Garbage-collect unreferenced blob files asynchronously

## 8. Milestones

### Milestone 0: Project bootstrap

- Initialize Cargo project
- Add config, error, logging
- Add health endpoint
- Add temp data directory support for tests

Exit criteria:

- service starts
- config loads
- integration test can boot the server

### Milestone 1: Metadata and blob storage

- Create SQLite schema
- Implement blob write/read/delete
- Implement bucket CRUD
- Implement object metadata CRUD

Exit criteria:

- object can be persisted and read after restart

### Milestone 2: S3-like basic API

- `PUT /bucket`
- `GET /buckets`
- `PUT /bucket/key`
- `GET /bucket/key`
- `HEAD /bucket/key`
- `DELETE /bucket/key`
- `GET /bucket?prefix=&delimiter=`

Exit criteria:

- integration tests cover bucket/object lifecycle

### Milestone 3: Robustness

- startup recovery for staging files
- concurrent writes to different keys
- overwrite semantics for same key
- request size limits
- checksum mismatch handling

Exit criteria:

- crash/restart tests pass
- concurrent API tests pass

### Milestone 4: Advanced object semantics

- multipart upload
- range read
- presigned URL or simple auth
- background garbage collection

Exit criteria:

- large file upload/download is stable

## 9. API Strategy

Do not chase full S3 compatibility at the start.

Recommended v1 API policy:

- use simple REST paths first
- keep request/response types explicit
- add compatibility adapters later if needed

Example:

- `PUT /buckets/{bucket}`
- `GET /buckets`
- `PUT /objects/{bucket}/{*key}`
- `GET /objects/{bucket}/{*key}`

After the storage engine is stable, a thin S3-compatible routing layer can be added.

## 10. Error Model

Define a strict error taxonomy early:

- `BucketNotFound`
- `BucketAlreadyExists`
- `ObjectNotFound`
- `InvalidObjectKey`
- `ChecksumMismatch`
- `StorageIo`
- `MetadataConflict`
- `Internal`

All handlers should map domain errors to stable HTTP status codes.

## 11. Testing Strategy

### Unit tests

- key validation
- metadata transaction behavior
- checksum logic
- blob path generation

### Integration tests

- create bucket, upload object, fetch object
- overwrite same key
- list with prefix/delimiter
- delete and verify disappearance
- restart recovery

### Stress and fault tests

- upload large files
- concurrent uploads
- kill/restart during staging commit
- invalid partial uploads

Recommended crates:

- `tempfile`
- `proptest`
- `reqwest`
- `tokio::test`

## 12. Technical Risks

Main risks to control early:

- inconsistent metadata/blob state after crashes
- blocking SQLite access on async runtime
- path traversal and invalid key handling
- memory growth during large uploads
- overwrite races on the same object key

Mitigations:

- always stream, never buffer full object in memory
- use staging + atomic rename
- serialize same-key writes if needed
- keep metadata updates transactional
- add restart recovery tests from the beginning

## 13. Suggested Dependencies

```toml
tokio
axum
serde
serde_json
thiserror
tracing
tracing-subscriber
uuid
sha2
hex
bytes
rusqlite
tempfile
reqwest
proptest
```

Optional:

- `blake3` for faster checksums
- `moka` for metadata cache

## 14. Development Order

Recommended implementation order:

1. config + errors + server bootstrap
2. SQLite schema and repository layer
3. blob store and staging writes
4. bucket service
5. object service
6. HTTP handlers
7. restart recovery
8. list semantics
9. multipart upload
10. auth/compatibility polish

## 15. Definition of Done for v1

The project can be considered a usable v1 when it satisfies all of the following:

- bucket/object CRUD works end to end
- data survives restart
- large files are streamed without full buffering
- overwrite/delete behavior is deterministic
- integration tests and recovery tests pass
- logs are sufficient to diagnose failures
