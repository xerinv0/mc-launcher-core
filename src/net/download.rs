//! Download plans and execution.

use std::{
    fs::{self, File},
    io,
    path::PathBuf,
    sync::Arc,
};

use tokio::{io::AsyncWriteExt, sync::Semaphore};

use crate::{
    io::hash::{sha1_file, sha1_file_async},
    progress::{ProgressEvent, ProgressReporter, SkipReason},
    LauncherError, Result,
};

/// Default number of concurrent downloads used by the async executor.
pub const DEFAULT_DOWNLOAD_WORKERS: usize = 16;

/// Supported checksum validation methods for downloaded files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Checksum {
    /// SHA-1 checksum.
    Sha1(String),
    /// SHA-256 checksum.
    Sha256(String),
}

/// One file download.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadTask {
    /// Source URL.
    pub url: String,
    /// Destination path.
    pub destination: PathBuf,
    /// Optional checksum used for skip and validation decisions.
    pub checksum: Option<Checksum>,
    /// Human-readable task label reported in progress events.
    pub label: String,
}

/// A batch of download tasks.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DownloadPlan {
    /// Tasks to execute in order.
    pub tasks: Vec<DownloadTask>,
}

/// Returns whether an existing destination file can be reused.
///
/// # Errors
///
/// Returns [`crate::LauncherError`] if checksum calculation fails.
pub fn should_skip_existing(task: &DownloadTask) -> Result<bool> {
    if !task.destination.is_file() {
        return Ok(false);
    }

    match &task.checksum {
        Some(Checksum::Sha1(expected)) => Ok(sha1_file(&task.destination)? == *expected),
        Some(Checksum::Sha256(_)) => Ok(false),
        None => Ok(true),
    }
}

/// Executes a download plan in order.
///
/// Existing files with matching checksums are skipped. Each completed SHA-1
/// download is verified before the next task begins.
///
/// # Errors
///
/// Returns [`crate::LauncherError`] for network, filesystem, or checksum
/// failures.
pub fn execute_plan(plan: &DownloadPlan, reporter: &mut dyn ProgressReporter) -> Result<()> {
    let client = super::http::client()?;
    for task in &plan.tasks {
        if should_skip_existing(task)? {
            reporter.report(ProgressEvent::TaskSkipped {
                label: task.label.clone(),
                reason: if task.checksum.is_some() {
                    SkipReason::ChecksumMatched
                } else {
                    SkipReason::FileExistsWithoutChecksum
                },
            });
            continue;
        }

        reporter.report(ProgressEvent::TaskStarted {
            label: task.label.clone(),
            path: task.destination.clone(),
        });

        if let Some(parent) = task.destination.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut response = client.get(&task.url).send()?.error_for_status()?;
        let mut file = File::create(&task.destination)?;
        io::copy(&mut response, &mut file)?;

        if let Some(Checksum::Sha1(expected)) = &task.checksum {
            let actual = sha1_file(&task.destination)?;
            if actual != *expected {
                return Err(LauncherError::ChecksumMismatch {
                    path: task.destination.clone(),
                    expected: expected.clone(),
                    actual,
                });
            }
        }

        reporter.report(ProgressEvent::TaskFinished {
            label: task.label.clone(),
        });
    }
    Ok(())
}

/// Executes a download plan concurrently from a tokio runtime.
///
/// Tasks are dispatched across the runtime's worker threads, with at most
/// `worker_count` downloads in flight at once. Skipped files are reported before
/// any download starts, matching the ordering of [`execute_plan`].
///
/// Progress events are reported from the caller's task as workers finish each
/// step, so labels can arrive in completion order rather than plan order.
/// [`ProgressEvent::BytesReceived`] is reported as each body streams to disk,
/// so callers can render accurate byte progress. Existing SHA-1 checksums are
/// still verified after the stream completes.
///
/// # Errors
///
/// Returns [`crate::LauncherError`] for a worker count of zero, network,
/// filesystem, or checksum failures. Other in-flight downloads are cancelled as
/// soon as the first error is observed.
pub async fn execute_plan_async(
    plan: &DownloadPlan,
    worker_count: usize,
    reporter: &mut dyn ProgressReporter,
) -> Result<()> {
    if worker_count == 0 {
        return Err(LauncherError::Other {
            message: "download worker count must be at least one".to_string(),
        });
    }
    if plan.tasks.is_empty() {
        return Ok(());
    }

    let mut to_download = Vec::new();
    for task in &plan.tasks {
        if should_skip_existing(task)? {
            reporter.report(ProgressEvent::TaskSkipped {
                label: task.label.clone(),
                reason: if task.checksum.is_some() {
                    SkipReason::ChecksumMatched
                } else {
                    SkipReason::FileExistsWithoutChecksum
                },
            });
        } else {
            to_download.push(task.clone());
        }
    }

    let client = super::http::async_client().await?;
    let limit = Arc::new(Semaphore::new(worker_count));
    let (tx, mut rx) = tokio::sync::mpsc::channel::<ProgressEvent>(to_download.len().max(1));
    let task_count = to_download.len();

    let mut set = tokio::task::JoinSet::new();
    for task in to_download {
        let client = client.clone();
        let limit = limit.clone();
        let tx = tx.clone();
        set.spawn(async move {
            let _permit = limit
                .acquire_owned()
                .await
                .map_err(|_| LauncherError::Other {
                    message: "download semaphore closed unexpectedly".to_string(),
                })?;
            run_download_task(&client, &tx, &task).await
        });
    }
    drop(tx);

    let mut completed = 0usize;
    let mut failure = None;
    while completed < task_count && failure.is_none() {
        tokio::select! {
            Some(event) = rx.recv() => reporter.report(event),
            joined = set.join_next() => match joined {
                Some(Ok(Ok(()))) => completed += 1,
                Some(Ok(Err(err))) => {
                    failure = Some(err);
                    set.abort_all();
                }
                Some(Err(_)) => {
                    failure = Some(LauncherError::Other {
                        message: "download worker panicked".to_string(),
                    });
                    set.abort_all();
                }
                None => break,
            }
        }
    }

    match failure {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

async fn run_download_task(
    client: &reqwest::Client,
    tx: &tokio::sync::mpsc::Sender<ProgressEvent>,
    task: &DownloadTask,
) -> Result<()> {
    tx.send(ProgressEvent::TaskStarted {
        label: task.label.clone(),
        path: task.destination.clone(),
    })
    .await
    .map_err(reporting_error)?;

    let result = stream_task_to_file(client, tx, task).await;
    if result.is_ok() {
        tx.send(ProgressEvent::TaskFinished {
            label: task.label.clone(),
        })
        .await
        .map_err(reporting_error)?;
    }
    result
}

async fn stream_task_to_file(
    client: &reqwest::Client,
    tx: &tokio::sync::mpsc::Sender<ProgressEvent>,
    task: &DownloadTask,
) -> Result<()> {
    if let Some(parent) = task.destination.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let mut response = client.get(&task.url).send().await?.error_for_status()?;
    let mut file = tokio::fs::File::create(&task.destination).await?;
    let total = response.content_length();
    let mut received = 0u64;
    while let Some(chunk) = response.chunk().await? {
        received += chunk.len() as u64;
        file.write_all(&chunk).await?;
        tx.send(ProgressEvent::BytesReceived {
            label: task.label.clone(),
            received,
            total,
        })
        .await
        .map_err(reporting_error)?;
    }
    file.flush().await?;

    if let Some(Checksum::Sha1(expected)) = &task.checksum {
        let actual = sha1_file_async(&task.destination).await?;
        if actual != *expected {
            return Err(LauncherError::ChecksumMismatch {
                path: task.destination.clone(),
                expected: expected.clone(),
                actual,
            });
        }
    }
    Ok(())
}

fn reporting_error<T>(_: tokio::sync::mpsc::error::SendError<T>) -> LauncherError {
    LauncherError::Other {
        message: "download progress receiver closed".to_string(),
    }
}
