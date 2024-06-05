use std::io::{Read, Write};
use std::sync::Arc;
use std::time::SystemTime;
use anyhow::{anyhow, bail, Context};
use dropbox_sdk::default_client::UserAuthDefaultClient;
use dropbox_sdk::files;
use dropbox_sdk::files::DownloadArg;
use dropbox_toolbox::ResultExt;
use crossbeam_channel::{unbounded, Receiver, Sender};
use threadpool::ThreadPool;
use time::Duration;
use crate::output::OUT;

pub struct Downloader {
    threads: ThreadPool,
}

pub struct DownloadRequest {
    pub path: String,
    pub rev: String,
    pub size: u64,
    pub mtime: i64,
    pub content_hash: String,
}

pub struct DownloadResult {
    pub path: String,
    pub size: u64,
    pub mtime: i64,
    pub content_hash: String,
    pub result: anyhow::Result<()>,
}

impl Downloader {
    pub fn new(base_path: String, client: Arc<UserAuthDefaultClient>) -> (Self, Sender<DownloadRequest>, Receiver<DownloadResult>) {
        let (jobs_tx, jobs_rx) = unbounded::<DownloadRequest>();
        let (results_tx, results_rx) = unbounded::<DownloadResult>();
        let threads = ThreadPool::default();

        for _ in 0 .. threads.max_count() {
            let jobs_rx = jobs_rx.clone();
            let results_tx = results_tx.clone();
            let base_path = base_path.clone();
            let client = client.clone();
            threads.execute(move || {
                while let Ok(job) = jobs_rx.recv() {
                    let mut result = DownloadResult {
                        path: job.path.clone(),
                        size: job.size,
                        mtime: job.mtime,
                        content_hash: job.content_hash.clone(),
                        result: Ok(()),
                    };

                    if let Err(e) = download(job.path.clone(), job.rev.clone(), job.mtime, job.size, &base_path, client.as_ref()) {
                        result.result = Err(e).context(format!("failed to download {}", job.path));
                    }

                    results_tx.send(result).unwrap();
                }
            });
        }

        (
            Self { threads },
            jobs_tx,
            results_rx,
        )
    }
}

fn download(path: String, rev: String, mtime: i64, size: u64, base_path: &str, client: &UserAuthDefaultClient) -> anyhow::Result<()> {
    OUT.get().unwrap().download_progress(&path, 0, size);

    let dl_path = base_path.to_owned() + &path;
    let result = files::download(client, &DownloadArg::new(dl_path).with_rev(rev), None, None)
        .combine()?;

    let mut src = result.body.ok_or_else(|| anyhow!("missing body in API download result"))?;
    let mut dest = crate::create_file(&path)?;

    // copy in chunks so we can update progress
    let mut written = 0u64;
    let mut buf = vec![0u8; 65536];
    loop {
        let r = src.read(&mut buf).context("failed to read")?;
        if r == 0 {
            break;
        }
        dest.write_all(&buf[0..r]).context("failed to write")?;
        written += r as u64;
        OUT.get().unwrap().download_progress(&path, written, size);
    }

    if written != size {
        bail!("written size ({written}) doesn't match expected size ({size})");
    }

    dest.set_modified(SystemTime::UNIX_EPOCH + Duration::seconds(mtime)).context("failed to set mtime")?;

    Ok(())
}

impl Drop for Downloader {
    fn drop(&mut self) {
        self.threads.join();
    }
}
