use std::{
    fs,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use sha2::{Digest, Sha256};

use crate::{AppError, AppResult, StorageLayout};

static BLOB_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
pub struct BlobStore {
    layout: StorageLayout,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredBlob {
    pub relative_path: String,
    pub size: u64,
    pub checksum: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PendingUpload {
    pub staging_path: PathBuf,
    token: String,
}

impl BlobStore {
    pub fn new(layout: StorageLayout) -> Self {
        Self { layout }
    }

    pub fn cleanup_staging(&self) -> AppResult<usize> {
        let mut cleaned = 0;

        for entry in fs::read_dir(&self.layout.staging_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                fs::remove_dir_all(&path)?;
            } else {
                fs::remove_file(&path)?;
            }

            cleaned += 1;
        }

        Ok(cleaned)
    }

    pub fn write_bytes(&self, bytes: &[u8]) -> AppResult<StoredBlob> {
        self.persist_bytes(bytes, |upload, size, checksum| {
            self.finalize_object_upload(upload, size, checksum)
        })
    }

    pub fn write_multipart_part(
        &self,
        upload_id: &str,
        part_number: u16,
        bytes: &[u8],
    ) -> AppResult<StoredBlob> {
        self.persist_bytes(bytes, |upload, size, checksum| {
            self.finalize_multipart_part_upload(upload, upload_id, part_number, size, checksum)
        })
    }

    pub fn compose_multipart(&self, part_paths: &[String]) -> AppResult<StoredBlob> {
        let upload = self.prepare_upload();
        let mut output = fs::File::create(&upload.staging_path)?;
        let mut hasher = Sha256::new();
        let mut total_size = 0_u64;
        let mut buffer = [0_u8; 8 * 1024];

        for relative_path in part_paths {
            let full_path = self.layout.resolve(relative_path);
            let mut input = open_existing_file(&full_path)?;

            loop {
                let read = input.read(&mut buffer)?;
                if read == 0 {
                    break;
                }

                output.write_all(&buffer[..read])?;
                hasher.update(&buffer[..read]);
                total_size += read as u64;
            }
        }

        output.sync_all()?;
        drop(output);

        let checksum = hex::encode(hasher.finalize());
        self.finalize_object_upload(&upload, total_size, checksum)
    }

    pub fn read_bytes(&self, relative_path: &str) -> AppResult<Vec<u8>> {
        let full_path = self.layout.resolve(relative_path);
        fs::read(&full_path).map_err(|error| map_missing_file(error, &full_path))
    }

    pub fn read_range(&self, relative_path: &str, start: u64, end: u64) -> AppResult<Vec<u8>> {
        let full_path = self.layout.resolve(relative_path);
        let mut file = open_existing_file(&full_path)?;
        file.seek(SeekFrom::Start(start))?;

        let mut remaining = end
            .checked_sub(start)
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| AppError::InvalidRange(format!("{start}-{end}")))?;
        let mut body = Vec::with_capacity(remaining as usize);
        let mut buffer = [0_u8; 8 * 1024];

        while remaining > 0 {
            let read_limit = remaining.min(buffer.len() as u64) as usize;
            let read = file.read(&mut buffer[..read_limit])?;
            if read == 0 {
                return Err(AppError::StorageInconsistent(format!(
                    "blob shorter than expected at {}",
                    full_path.display()
                )));
            }

            body.extend_from_slice(&buffer[..read]);
            remaining -= read as u64;
        }

        Ok(body)
    }

    pub fn delete_blob(&self, relative_path: &str) -> AppResult<()> {
        let full_path = self.layout.resolve(relative_path);
        self.delete_path_if_exists(&full_path)
    }

    pub(crate) fn prepare_upload(&self) -> PendingUpload {
        let token = unique_token();
        PendingUpload {
            staging_path: self.layout.staging_dir.join(format!("upload-{token}.part")),
            token,
        }
    }

    pub(crate) fn cleanup_upload(&self, upload: &PendingUpload) -> AppResult<()> {
        self.delete_path_if_exists(&upload.staging_path)
    }

    pub(crate) fn finalize_object_upload(
        &self,
        upload: &PendingUpload,
        size: u64,
        checksum: String,
    ) -> AppResult<StoredBlob> {
        let relative_path = format!(
            "blobs/{}/{}-{}.blob",
            &checksum[..2],
            &checksum[..16],
            upload.token
        );
        self.finalize_upload(upload, relative_path, size, checksum)
    }

    pub(crate) fn finalize_multipart_part_upload(
        &self,
        upload: &PendingUpload,
        upload_id: &str,
        part_number: u16,
        size: u64,
        checksum: String,
    ) -> AppResult<StoredBlob> {
        let relative_path = format!(
            "staging/multipart/{upload_id}/part-{part_number:05}-{}.part",
            upload.token
        );
        self.finalize_upload(upload, relative_path, size, checksum)
    }

    fn finalize_upload(
        &self,
        upload: &PendingUpload,
        relative_path: String,
        size: u64,
        checksum: String,
    ) -> AppResult<StoredBlob> {
        let final_path = self.layout.resolve(&relative_path);
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::rename(&upload.staging_path, &final_path)?;

        Ok(StoredBlob {
            relative_path,
            size,
            checksum,
        })
    }

    fn delete_path_if_exists(&self, path: &Path) -> AppResult<()> {
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn persist_bytes<F>(&self, bytes: &[u8], finalize: F) -> AppResult<StoredBlob>
    where
        F: FnOnce(&PendingUpload, u64, String) -> AppResult<StoredBlob>,
    {
        let upload = self.prepare_upload();
        let checksum = hash_bytes(bytes);

        {
            let mut file = fs::File::create(&upload.staging_path)?;
            file.write_all(bytes)?;
            file.sync_all()?;
        }

        finalize(&upload, bytes.len() as u64, checksum).inspect_err(|_| {
            let _ = self.cleanup_upload(&upload);
        })
    }
}

fn open_existing_file(path: &Path) -> AppResult<fs::File> {
    fs::File::open(path).map_err(|error| map_missing_file(error, path))
}

fn map_missing_file(error: std::io::Error, path: &Path) -> AppError {
    match error.kind() {
        std::io::ErrorKind::NotFound => {
            AppError::StorageInconsistent(format!("blob missing at {}", path.display()))
        }
        _ => error.into(),
    }
}

fn hash_bytes(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn unique_token() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should move forward")
        .as_nanos();
    let counter = BLOB_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{nanos:x}-{}-{counter:x}", std::process::id())
}
