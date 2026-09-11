//! Asynchronous install and launch with a live download progress bar.
//!
//! Runs inside a tokio runtime whose multi-threaded worker pool drives the
//! concurrent downloads. The [`ProgressReporter`] passed to
//! [`Launcher::install_with_progress_async`] renders an overall task bar and an
//! aggregate byte bar as the plan executes. Blocking work such as native
//! extraction and loader installer execution is offloaded to the runtime's
//! blocking pool.
//!
//! Requires the `indicatif` dev-dependency for the progress bars.

use std::{collections::HashMap, process::Command};

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use mc_launcher_core::prelude::*;

/// Renders install progress on two bars: one for the overall task count and
/// one for the aggregate bytes streamed across all concurrent downloads.
struct InstallProgress {
    multi: MultiProgress,
    tasks: ProgressBar,
    bytes: ProgressBar,
    details: HashMap<String, (u64, Option<u64>)>,
}

impl InstallProgress {
    fn new() -> Self {
        let multi = MultiProgress::new();

        let tasks = multi.add(ProgressBar::new(0));
        tasks.set_style(
            ProgressStyle::with_template("{msg} {wide_bar} {pos}/{len} tasks {elapsed}")
                .expect("valid task bar template")
                .progress_chars("##-"),
        );
        tasks.set_message("install");

        let bytes = multi.add(ProgressBar::new(0));
        bytes.set_style(
            ProgressStyle::with_template("{msg} {wide_bar} {bytes}/{total_bytes} {bytes_per_sec}")
                .expect("valid byte bar template")
                .progress_chars("##-"),
        );
        bytes.set_message("waiting");

        Self {
            multi,
            tasks,
            bytes,
            details: HashMap::new(),
        }
    }

    fn refresh_bytes(&self) {
        let mut received = 0u64;
        let mut total = 0u64;
        for (recv, task_total) in self.details.values() {
            let Some(task_total) = task_total else {
                return;
            };
            received += recv;
            total += task_total;
        }
        if total == 0 {
            return;
        }
        self.bytes.set_length(total.max(received));
        self.bytes.set_position(received);
    }

    fn finish(self) {
        self.tasks.finish_and_clear();
        self.bytes.finish_and_clear();
        self.multi.clear().expect("clear progress bars");
    }
}

impl ProgressReporter for InstallProgress {
    fn report(&mut self, event: ProgressEvent) {
        match event {
            ProgressEvent::StageStarted { .. } => {}
            ProgressEvent::TaskStarted { label, .. } => {
                self.tasks.inc_length(1);
                self.bytes.set_message(label);
            }
            ProgressEvent::TaskSkipped { label, .. } => {
                self.tasks.inc_length(1);
                self.tasks.inc(1);
                self.bytes.set_message(format!("skipped {label}"));
            }
            ProgressEvent::BytesReceived {
                label,
                received,
                total,
                ..
            } => {
                self.details.insert(label, (received, total));
                self.refresh_bytes();
            }
            ProgressEvent::TaskFinished { label } => {
                self.details.remove(&label);
                self.tasks.inc(1);
                self.refresh_bytes();
            }
        }
    }
}

#[tokio::main]
async fn main() -> mc_launcher_core::Result<()> {
    let minecraft_dir = std::env::current_dir()?.join(".minecraft");
    let launcher = Launcher::new(minecraft_dir);

    let mut progress = InstallProgress::new();
    let install = launcher
        .install_with_progress_async(
            InstallRequest {
                minecraft_version: "1.20.1".to_string(),
                loader: Some(LoaderSpec::Fabric {
                    version: LoaderVersion::LatestStable,
                }),
                java: JavaInstallPolicy::Auto,
            },
            &mut progress,
        )
        .await;
    progress.finish();
    let install = install?;

    println!("installed profile: {}", install.version_id);

    let version = launcher.load_version(&install.version_id)?;
    let command = launcher.build_launch_command_from_version(
        &version,
        LaunchOptions {
            account: Account::offline("Steve"),
            ..Default::default()
        },
    )?;

    println!("launching {}", command.executable.display());
    let mut child = Command::new(&command.executable)
        .args(&command.args)
        .current_dir(&command.working_dir)
        .spawn()?;
    child.wait()?;
    Ok(())
}
