use std::collections::HashMap;

use anyhow::Result;
use thiserror::Error;

#[derive(Clone, Debug)]
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
}

pub trait Store {
    fn get(&self, key: &str) -> Result<Blob>;
    fn put(&mut self, key: &str, value: Blob) -> Result<()>;
}
