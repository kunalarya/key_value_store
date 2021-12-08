mod file_store;
mod load_test;
mod mem_store;
mod store;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Result};
use structopt::StructOpt;

use crate::mem_store::MemoryStore;

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

    /// Emulated load pattern.
    #[structopt(long, default_value = "consistent")]
    pattern: load_test::LoadPattern,

    /// How long to generate loads for.
    #[structopt(long, default_value = "60")]
    load_time_sec: u64,
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

        /// How often to persist changes to disk, in microseconds. Implies synchronous writing;
        /// mutually exclusive with queue_depth.
        #[structopt(long)]
        write_period_us: Option<u64>,

        /// The number of in-flight requests queued up to write to disk. Implies asynchronous
        /// writing; mutually exclusive with write_period_us.
        #[structopt(long)]
        queue_depth: Option<usize>,

        /// Target file format.
        #[structopt(long, default_value = "json")]
        serializer: file_store::Serializer,
    },
}

fn run(opts: LoadTestOptions) -> Result<()> {
    let load_params = load_test::LoadParams {
        threads: opts.threads,
        load_pattern: opts.pattern,
        tot_time: Duration::from_secs(opts.load_time_sec),
    };
    let all_stats = match opts.backend {
        Backend::Memory => load_test::load_test(MemoryStore::new(), load_params),
        Backend::File {
            output,
            file_count,
            write_period_us,
            queue_depth,
            serializer,
        } => {
            let (output_path, _tmp_path) = if let Some(output_path) = output {
                (output_path, None)
            } else {
                let tmp_path = tempfile::tempdir()?;
                (tmp_path.path().to_path_buf(), Some(tmp_path))
            };

            if write_period_us.is_some() && queue_depth.is_some() {
                bail!("Cannot set both write_period_us and queue_depth");
            }

            let write_policy = if let Some(write_period_us) = write_period_us {
                file_store::WritePolicy::Synchronous {
                    write_period: Duration::from_micros(write_period_us),
                }
            } else if let Some(queue_depth) = queue_depth {
                file_store::WritePolicy::Asynchronous { queue_depth }
            } else {
                bail!("Must set either a queue depth or write period");
            };

            let backend =
                file_store::FileStore::new(&output_path, file_count, &write_policy, serializer)?;
            load_test::load_test(backend, load_params)
        }
    }?;

    load_test::summarize(&all_stats)?;
    Ok(())
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let opt = LoadTestOptions::from_args();
    log::info!("Using config: {:#?}", opt);
    run(opt)?;
    Ok(())
}
