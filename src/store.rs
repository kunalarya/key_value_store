use std::collections::HashMap;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Blob {
    Null,
    Str(String),
    Int(isize),
    Dict(HashMap<String, Blob>),
}

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("lock failed to acquire")]
    LockError,
    #[error("bad file hash: {0}")]
    BadFileHash(usize),
    #[error("no threads completed")]
    NoThreadsCompleted,
}

pub trait Store: Sized + Send {
    fn get(&self, key: &str) -> Result<Blob>;
    fn put(&mut self, key: &str, value: Blob) -> Result<()>;
    fn spawn(&mut self) -> Result<Self>;
}
