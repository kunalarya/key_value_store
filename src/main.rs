mod file_store;
mod mem_store;
mod store;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use structopt::StructOpt;

use crate::mem_store::MemoryStore;
use crate::store::{Blob, Store};

/// Run different key-value store implementations under load.
#[derive(StructOpt, Debug)]
#[structopt(name = "key_value_store")]
struct LoadTestOptions {
    /// Number of threads.
    #[structopt(short, long, default_value = "100")]
    threads: usize,

    /// Type of backend.
    #[structopt(subcommand)]
    backend: Backend,
}

#[derive(StructOpt, Debug)]
enum Backend {
    Memory,
    File {
        /// Output path for file-based backends. Defaults to tmp.
        #[structopt(long)]
        output: Option<PathBuf>,

        /// Number of files to shard across.
        #[structopt(long)]
        file_count: usize,

        /// How often to persist changes to disk, in microseconds.
        #[structopt(long, default_value = "500")]
        flush_period_us: u64,

        /// Target file format.
        #[structopt(long, default_value = "json")]
        serializer: file_store::Serializer,
    },
}

fn run(opts: LoadTestOptions) -> Result<()> {
    match opts.backend {
        Backend::Memory => load_test(opts.threads, MemoryStore::new()),
        Backend::File {
            output,
            file_count,
            flush_period_us,
            serializer,
        } => {
            let (output_path, _tmp_path) = if let Some(output_path) = output {
                (output_path, None)
            } else {
                let tmp_path = tempfile::tempdir()?;
                (tmp_path.path().to_path_buf(), Some(tmp_path))
            };

            let flush_period = Duration::from_micros(flush_period_us);
            let backend =
                file_store::FileStore::new(&output_path, file_count, flush_period, serializer)?;
            load_test(opts.threads, backend)
        }
    }
}

fn single_tester<S: Store>(mut store: S) -> Result<()> {
    store.put("foo", Blob::Str("foo".to_string()))?;
    Ok(())
}

fn load_test<S: Store>(threads: usize, mut store: S) -> Result<()> {
    use crossbeam::thread;
    thread::scope(|s| {
        for _ in 0..threads {
            let thread_store = store.spawn().expect("Could not spawn store.");
            s.spawn(|_| single_tester(thread_store));
        }
    })
    .unwrap();
    Ok(())
}

fn main() -> Result<()> {
    env_logger::init();

    let opt = LoadTestOptions::from_args();
    println!("{:?}", opt);
    run(opt)?;
    Ok(())
}
