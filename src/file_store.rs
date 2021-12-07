use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::mem_store::MemoryStoreSingleThreaded;
use crate::store::{Blob, Store, StoreError};

#[derive(Clone, Debug)]
pub enum Serializer {
    Json,
}

impl std::str::FromStr for Serializer {
    type Err = StoreError;

    fn from_str(s: &str) -> std::result::Result<Serializer, StoreError> {
        match s.to_lowercase().trim() {
            "json" => Ok(Self::Json),
            _ => Err(StoreError::UnknownSerializer(s.to_owned())),
        }
    }
}

impl Serializer {
    fn write<T: Serialize, W: Write>(&self, writer: W, value: &T) -> Result<()> {
        #[allow(clippy::match_single_binding)]
        match self {
            Self::Json => serde_json::to_writer(writer, value)?,
            // Add new serialization formats here.
        };
        Ok(())
    }

    fn read<T: DeserializeOwned, R: Read>(&self, reader: R) -> Result<T> {
        #[allow(clippy::match_single_binding)]
        Ok(match self {
            Self::Json => serde_json::from_reader(reader)?,
            // Add new serialization formats here.
        })
    }
}

/// Simple hasher to determine the output file for a given key.
#[derive(Clone, Debug)]
struct SimpleHasher {
    max_values: usize,
}

impl SimpleHasher {
    fn new(max_values: usize) -> Self {
        Self { max_values }
    }
}

impl SimpleHasher {
    fn hash_key(&self, key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        // In production code, we'd use u64 everywhere to be explicit; for now
        // we'll stick with usize for simplicity.
        (hash as usize) % self.max_values
    }
}

struct Poller {
    period: Duration,
    last_time: Instant,
}

impl Poller {
    fn new(period: Duration) -> Self {
        Self {
            period,
            last_time: Instant::now(),
        }
    }

    fn elapsed(&mut self) -> bool {
        let now = Instant::now();
        if now - self.last_time >= self.period {
            self.last_time = now;
            true
        } else {
            false
        }
    }
}

/// Internal representation to encapsulate file operations.
struct BackingFile {
    file: File,
    mem_store: MemoryStoreSingleThreaded,
    flush_poller: Poller,
    serializer: Serializer,
}

impl BackingFile {
    fn new(
        size: usize,
        index: usize,
        path: &Path,
        flush_period: Duration,
        serializer: Serializer,
    ) -> Result<Self> {
        // TODO: Use file locks, otherwise multiple threads creating backing files could
        // cause odd issues.
        let basename = format!("store_size={}_idx={}", size, index);
        let filename = path.join(basename);
        // If the file already exists, load it from memory.
        let mem_store = if filename.exists() {
            log::info!(
                "File {:?} already exists. Attempting to load previous data.",
                filename
            );
            // Try to read existing data.
            let existing_file = File::open(&filename)?;
            match serializer.read(existing_file) {
                Ok(existing_data) => existing_data,
                Err(err) => {
                    // TODO Rename with timestamp
                    log::error!(
                        "Could not load key/value store data from {:?}; skipping: {:?}",
                        filename,
                        err
                    );
                    let now = chrono::Local::now();
                    let timestamp = now.format("%Y-%m-%d_%H%M%S");
                    let backup_filename = filename.with_extension(format!("backup{}", timestamp));
                    std::fs::rename(&filename, backup_filename)?;
                    MemoryStoreSingleThreaded::new()
                }
            }
        } else {
            MemoryStoreSingleThreaded::new()
        };
        let file = File::create(&filename)?;
        Ok(Self {
            file,
            mem_store,
            flush_poller: Poller::new(flush_period),
            serializer,
        })
    }

    fn read(&self, key: &str) -> Result<Blob> {
        self.mem_store.get(key)
    }

    fn write(&mut self, key: &str, value: Blob) -> Result<()> {
        self.mem_store.put(key, value)?;
        // Flush if enough time has passed.
        if self.flush_poller.elapsed() {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.serializer.write(&self.file, &self.mem_store)?;
        Ok(())
    }
}

pub struct FileStore {
    files: Vec<Arc<Mutex<BackingFile>>>,
    hasher: SimpleHasher,
}

impl FileStore {
    // TODO: Use a builder.
    pub fn new(
        output_path: &Path,
        file_count: usize,
        flush_period: Duration,
        serializer: Serializer,
    ) -> Result<Self> {
        // Preinitialize backing stores.
        let mut files = Vec::with_capacity(file_count);
        for index in 0..file_count {
            files.push(Arc::new(Mutex::new(BackingFile::new(
                file_count,
                index,
                output_path,
                flush_period,
                serializer.clone(),
            )?)));
        }
        Ok(Self {
            files,
            hasher: SimpleHasher::new(file_count),
        })
    }
}

impl Store for FileStore {
    fn get(&self, key: &str) -> Result<Blob> {
        let index = self.hasher.hash_key(key);
        let file = self
            .files
            .get(index)
            .ok_or(StoreError::BadFileHash(index))?;
        {
            let guard = file.lock().map_err(|_| StoreError::LockError)?;
            guard.read(key)
        }
    }

    fn put(&mut self, key: &str, value: Blob) -> Result<()> {
        let index = self.hasher.hash_key(key);
        let file = self
            .files
            .get(index)
            .ok_or(StoreError::BadFileHash(index))?;
        // Minimizing the length of time we hold the lock for.
        {
            let mut guard = file.lock().map_err(|_| StoreError::LockError)?;
            guard.write(key, value)
        }
    }

    fn spawn(&mut self) -> Result<Self> {
        Ok(Self {
            files: self.files.iter().map(Arc::clone).collect(),
            hasher: self.hasher.clone(),
        })
    }
}
