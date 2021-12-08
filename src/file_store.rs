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
use structopt::clap::arg_enum;

use crate::mem_store::MemoryStoreSingleThreaded;
use crate::store::{Blob, Store, StoreError};

arg_enum! {
    #[derive(Clone, Debug)]
    pub enum Serializer {
        Json,
        // TODO: Add ciborium and speedy as options.
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

pub enum WritePolicy {
    Synchronous { write_period: Duration },
    Asynchronous { queue_depth: usize },
}

enum Writer {
    Synchronous {
        poller: Poller,
        serializer: Serializer,
        file: File,
    },
    Asynchronous {
        sender: crossbeam_channel::Sender<(String, Blob)>,
        _handle: std::thread::JoinHandle<()>,
    },
}

impl Writer {
    fn new(
        policy: &WritePolicy,
        mem_store: &MemoryStoreSingleThreaded,
        serializer: Serializer,
        filename: &Path,
    ) -> Result<Self> {
        let file = File::create(&filename)?;
        let writer = match policy {
            WritePolicy::Synchronous { write_period } => {
                let poller = Poller::new(*write_period);
                Self::Synchronous {
                    poller,
                    serializer,
                    file,
                }
            }
            WritePolicy::Asynchronous { queue_depth } => {
                #[allow(clippy::type_complexity)]
                let (sender, receiver): (
                    crossbeam_channel::Sender<(String, Blob)>,
                    crossbeam_channel::Receiver<(String, Blob)>,
                ) = crossbeam_channel::bounded(*queue_depth);

                // Keep a copy of the memstore state in the background thread.
                let mut async_writer_mem_store_mirror = mem_store.clone();

                let handle = std::thread::spawn(move || loop {
                    if let Ok((key, value)) = receiver.recv() {
                        if let Err(err) = async_writer_mem_store_mirror.put(&key, value) {
                            // TODO: Hard failure.
                            log::error!("put error: {:?}", err);
                        }
                        if let Err(err) = serializer.write(&file, &async_writer_mem_store_mirror) {
                            // TODO: This should be a hard failure; we can imagine an "errors"
                            // return channel that dequeues any pending write errors and handles
                            // them appropriately.
                            log::error!("write error: {:?}", err);
                        }
                    }
                });
                Self::Asynchronous {
                    _handle: handle,
                    sender,
                }
            }
        };
        Ok(writer)
    }

    fn write(
        &mut self,
        key: &str,
        value: &Blob,
        mem_store: &MemoryStoreSingleThreaded,
    ) -> Result<()> {
        match self {
            Writer::Synchronous {
                poller,
                file,
                serializer,
            } => {
                if poller.elapsed() {
                    serializer.write(file, mem_store)?;
                }
            }
            Writer::Asynchronous { sender, .. } => {
                sender.send((key.to_owned(), value.clone()))?;
            }
        };
        Ok(())
    }
}

/// Internal representation to encapsulate file operations.
struct BackingFile {
    mem_store: MemoryStoreSingleThreaded,
    writer: Writer,
}

impl BackingFile {
    fn new(
        size: usize,
        index: usize,
        path: &Path,
        write_policy: &WritePolicy,
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

        let writer = Writer::new(write_policy, &mem_store, serializer, &filename)?;

        Ok(Self { mem_store, writer })
    }

    fn read(&self, key: &str) -> Result<Blob> {
        self.mem_store.get(key)
    }

    fn write(&mut self, key: &str, value: Blob) -> Result<()> {
        self.writer.write(key, &value, &self.mem_store)?;
        self.mem_store.put(key, value)?;
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
        write_policy: &WritePolicy,
        serializer: Serializer,
    ) -> Result<Self> {
        // Preinitialize backing stores.
        let mut files = Vec::with_capacity(file_count);
        for index in 0..file_count {
            files.push(Arc::new(Mutex::new(BackingFile::new(
                file_count,
                index,
                output_path,
                write_policy,
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
