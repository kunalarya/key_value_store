mod file_store;
mod load_test;
mod mem_store;
mod store;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use structopt::StructOpt;

use crate::mem_store::MemoryStore;
use crate::store::StoreError;

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

        /// How often to persist changes to disk, in microseconds.
        #[structopt(long, default_value = "500")]
        flush_period_us: u64,

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
            load_test::load_test(backend, load_params)
        }
    }?;

    summarize(&all_stats)?;
    Ok(())
}

fn summarize(all_stats: &[load_test::Stats]) -> Result<()> {
    let total_ops: i64 = all_stats.iter().map(|s| s.ops.0).sum();
    let total_runtime = all_stats
        .iter()
        .map(|s| s.runtime)
        .max()
        .ok_or(StoreError::NoThreadsCompleted)?;

    let total_ops_per_sec = total_ops as f64 / total_runtime.as_secs_f64();

    log::info!("total_ops: {}", total_ops);
    log::info!("total_runtime: {:?}", total_runtime);
    log::info!("total_ops_per_sec: {}", total_ops_per_sec);
    Ok(())
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let opt = LoadTestOptions::from_args();
    log::info!("Using config: {:#?}", opt);
    run(opt)?;
    Ok(())
}
