mod mem_store;
mod store;

use anyhow::Result;

use mem_store::MemoryStore;
use store::{Blob, Store};

fn main() -> Result<()> {
    let mut mem_store = MemoryStore::new();
    mem_store.put("foo", Blob::Str("foo".to_string()))?;
    println!("{:?}", mem_store.get("foo")?);
    Ok(())
}
