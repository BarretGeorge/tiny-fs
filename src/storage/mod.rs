mod blob_store;

use std::path::PathBuf;

pub use blob_store::{BlobStore, StoredBlob};

#[derive(Clone, Debug)]
pub struct StorageLayout {
    pub root: PathBuf,
    pub blobs_dir: PathBuf,
    pub staging_dir: PathBuf,
    pub meta_db_path: PathBuf,
}

impl StorageLayout {
    pub fn initialize(root: impl Into<PathBuf>) -> std::io::Result<Self> {
        let layout = Self::from_root(root.into());

        std::fs::create_dir_all(&layout.blobs_dir)?;
        std::fs::create_dir_all(&layout.staging_dir)?;

        Ok(layout)
    }

    pub fn from_root(root: PathBuf) -> Self {
        Self {
            blobs_dir: root.join("blobs"),
            staging_dir: root.join("staging"),
            meta_db_path: root.join("meta.db"),
            root,
        }
    }

    pub fn resolve(&self, relative_path: &str) -> PathBuf {
        self.root.join(relative_path)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::StorageLayout;

    fn unique_temp_path(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should move forward")
            .as_nanos();
        env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()))
    }

    #[test]
    fn initialize_creates_expected_directories() {
        let root = unique_temp_path("tiny-fs-storage-test");

        let layout = StorageLayout::initialize(root.clone()).expect("layout should initialize");

        assert_eq!(layout.root, root);
        assert!(layout.blobs_dir.is_dir());
        assert!(layout.staging_dir.is_dir());
        assert_eq!(layout.meta_db_path, root.join("meta.db"));

        std::fs::remove_dir_all(root).expect("test directory should be removable");
    }
}
