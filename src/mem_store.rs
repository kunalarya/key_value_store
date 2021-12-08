use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

use crate::store::{Blob, Store, StoreError};

/// An incredibly simple in-memory store for storing/retrieving information.
/// Useful for testing.
pub struct MemoryStore {
    values: Arc<Mutex<HashMap<String, Blob>>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            values: Arc::new(Mutex::new(HashMap::with_capacity(128))),
        }
    }
}

impl Store for MemoryStore {
    fn get(&self, key: &str) -> Result<Blob> {
        let values = self.values.lock().map_err(|_| StoreError::LockError)?;
        if let Some(value) = values.get(key) {
            Ok(value.clone())
        } else {
            bail!("Key not found: {}", key)
        }
    }

    fn put(&mut self, key: &str, value: Blob) -> Result<()> {
        let mut values = self.values.lock().map_err(|_| StoreError::LockError)?;
        values.insert(key.to_string(), value);
        Ok(())
    }

    fn spawn(&mut self) -> Result<Self> {
        Ok(MemoryStore {
            values: Arc::clone(&self.values),
        })
    }
}

/// Same as MemoryStore, but not thread safe.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MemoryStoreSingleThreaded {
    values: HashMap<String, Blob>,
}

impl MemoryStoreSingleThreaded {
    pub fn new() -> Self {
        Self {
            values: HashMap::with_capacity(128),
        }
    }
}

impl Store for MemoryStoreSingleThreaded {
    fn get(&self, key: &str) -> Result<Blob> {
        if let Some(value) = self.values.get(key) {
            Ok(value.clone())
        } else {
            bail!("Key not found: {}", key)
        }
    }

    fn put(&mut self, key: &str, value: Blob) -> Result<()> {
        self.values.insert(key.to_string(), value);
        Ok(())
    }

    fn spawn(&mut self) -> Result<Self> {
        bail!("Spawning is not supported on {:?}", self)
    }
}
