//! High-level launcher facade.
//!
//! [`Launcher`] owns a Minecraft directory and coordinates the common workflow:
//! install a profile, load its merged version metadata, and build a Java
//! command from that metadata.

use std::path::{Path, PathBuf};

use crate::{
    command::builder::{build_launch_command, LaunchCommand, LaunchOptions},
    core::version::VersionJson,
    install::{
        client::{
            fetch_vanilla_version, fetch_vanilla_version_async, install_version_files,
            install_version_files_async, load_version_json, write_version_json,
        },
        loader::{run_loader_installer, write_loader_profile, InstallerInvocation},
        request::{InstallRequest, InstallResult},
    },
    loader::{
        common::{LoaderSpec, LoaderVersion},
        LoaderKind,
    },
    net::download::{
        execute_plan, execute_plan_async, DownloadPlan, DownloadTask, DEFAULT_DOWNLOAD_WORKERS,
    },
    progress::{ProgressEvent, ProgressReporter},
    LauncherError, Result,
};

/// Facade for installing and launching Minecraft profiles inside one directory.
///
/// A `Launcher` is cheap to clone and only stores the root Minecraft directory.
/// The directory is expected to follow the standard launcher layout with
/// `versions`, `libraries`, `assets`, and `runtime` children as needed.
#[derive(Debug, Clone)]
pub struct Launcher {
    minecraft_dir: PathBuf,
}

impl Launcher {
    /// Creates a launcher rooted at the given Minecraft directory.
    ///
    /// The directory is created lazily by install operations.
    pub fn new(minecraft_dir: impl Into<PathBuf>) -> Self {
        Self {
            minecraft_dir: minecraft_dir.into(),
        }
    }

    /// Returns the Minecraft directory managed by this launcher.
    pub fn minecraft_dir(&self) -> &Path {
        &self.minecraft_dir
    }

    /// Installs a vanilla or loader-backed Minecraft profile.
    ///
    /// This is a convenience wrapper around [`Launcher::install_with_progress`]
    /// that ignores progress events.
    ///
    /// # Errors
    ///
    /// Returns [`LauncherError`] for network, filesystem, metadata, checksum, or
    /// loader-installer failures.
    pub fn install(&self, request: InstallRequest) -> Result<InstallResult> {
        let mut reporter = |_event: ProgressEvent| {};
        self.install_with_progress(request, &mut reporter)
    }

    /// Installs a profile and reports progress as tasks are processed.
    ///
    /// Vanilla, Fabric, and Quilt installs are handled with Rust-native
    /// metadata planning. Forge and NeoForge currently download the installer
    /// jar and invoke it with `java`.
    ///
    /// # Errors
    ///
    /// Returns [`LauncherError`] for invalid versions, failed downloads,
    /// checksum mismatches, unsafe paths, or loader installer failures.
    pub fn install_with_progress(
        &self,
        request: InstallRequest,
        reporter: &mut dyn ProgressReporter,
    ) -> Result<InstallResult> {
        if let Some(loader) = request.loader.clone() {
            match loader {
                LoaderSpec::Fabric { version } => {
                    self.install_vanilla_version(&request.minecraft_version, reporter)?;
                    let loader_version = resolve_fabric_loader_version(version)?;
                    let profile = crate::loader::fabric::fetch_profile(
                        &request.minecraft_version,
                        &loader_version,
                    )?;
                    let version_id = version_id(&profile, "loader profile")?.to_string();
                    write_loader_profile(&self.minecraft_dir, &profile)?;
                    let merged = self.load_version(&version_id)?;
                    install_version_files(&merged, &self.minecraft_dir, reporter)?;
                    return Ok(InstallResult { version_id });
                }
                LoaderSpec::Quilt { version } => {
                    self.install_vanilla_version(&request.minecraft_version, reporter)?;
                    let loader_version = resolve_quilt_loader_version(version)?;
                    let profile = crate::loader::quilt::fetch_profile(
                        &request.minecraft_version,
                        &loader_version,
                    )?;
                    let version_id = version_id(&profile, "loader profile")?.to_string();
                    write_loader_profile(&self.minecraft_dir, &profile)?;
                    let merged = self.load_version(&version_id)?;
                    install_version_files(&merged, &self.minecraft_dir, reporter)?;
                    return Ok(InstallResult { version_id });
                }
                LoaderSpec::Forge { version } => {
                    self.install_vanilla_version(&request.minecraft_version, reporter)?;
                    let loader_version =
                        resolve_forge_loader_version(&request.minecraft_version, version)?;
                    let installer_path = download_installer(
                        &self.minecraft_dir,
                        "forge",
                        &loader_version,
                        &crate::loader::forge::installer_url(&loader_version),
                    )?;
                    run_loader_installer(&InstallerInvocation {
                        loader: LoaderKind::Forge,
                        java_executable: PathBuf::from("java"),
                        installer_path,
                        minecraft_dir: self.minecraft_dir.clone(),
                    })?;
                    let version_id =
                        crate::loader::forge::forge_installed_version_id(&loader_version)?;
                    let merged = self.load_version(&version_id)?;
                    install_version_files(&merged, &self.minecraft_dir, reporter)?;
                    return Ok(InstallResult { version_id });
                }
                LoaderSpec::NeoForge { version } => {
                    self.install_vanilla_version(&request.minecraft_version, reporter)?;
                    let loader_version =
                        resolve_neoforge_loader_version(&request.minecraft_version, version)?;
                    let installer_path = download_installer(
                        &self.minecraft_dir,
                        "neoforge",
                        &loader_version,
                        &crate::loader::neoforge::installer_url(&loader_version),
                    )?;
                    run_loader_installer(&InstallerInvocation {
                        loader: LoaderKind::NeoForge,
                        java_executable: PathBuf::from("java"),
                        installer_path,
                        minecraft_dir: self.minecraft_dir.clone(),
                    })?;
                    let version_id = crate::loader::neoforge::neoforge_installed_version_id(
                        &request.minecraft_version,
                        &loader_version,
                    );
                    let merged = self.load_version(&version_id)?;
                    install_version_files(&merged, &self.minecraft_dir, reporter)?;
                    return Ok(InstallResult { version_id });
                }
            }
        }

        self.install_vanilla_version(&request.minecraft_version, reporter)?;
        Ok(InstallResult {
            version_id: request.minecraft_version,
        })
    }

    /// Installs a vanilla or loader-backed Minecraft profile using concurrent
    /// async downloads.
    ///
    /// This is a convenience wrapper around
    /// [`Launcher::install_with_progress_async`] that ignores progress events.
    /// It must be called from within a tokio runtime; the runtime's worker pool
    /// drives the parallel downloads and blocking file work is offloaded to its
    /// blocking pool.
    ///
    /// # Errors
    ///
    /// Returns [`LauncherError`] for network, filesystem, metadata, checksum, or
    /// loader-installer failures.
    pub async fn install_async(&self, request: InstallRequest) -> Result<InstallResult> {
        let mut reporter = |_event: ProgressEvent| {};
        self.install_with_progress_async(request, &mut reporter)
            .await
    }

    /// Installs a profile and reports progress, using concurrent async downloads.
    ///
    /// Mirrors [`Launcher::install_with_progress`] for async callers. The plan
    /// is executed by [`crate::net::download::execute_plan_async`] with
    /// [`crate::net::download::DEFAULT_DOWNLOAD_WORKERS`] workers.
    ///
    /// # Errors
    ///
    /// Returns [`LauncherError`] for invalid versions, failed downloads,
    /// checksum mismatches, unsafe paths, or loader installer failures.
    pub async fn install_with_progress_async(
        &self,
        request: InstallRequest,
        reporter: &mut dyn ProgressReporter,
    ) -> Result<InstallResult> {
        if let Some(loader) = request.loader.clone() {
            match loader {
                LoaderSpec::Fabric { version } => {
                    self.install_vanilla_version_async(&request.minecraft_version, reporter)
                        .await?;
                    let loader_version = resolve_fabric_loader_version_async(version).await?;
                    let profile = crate::loader::fabric::fetch_profile_async(
                        &request.minecraft_version,
                        &loader_version,
                    )
                    .await?;
                    let version_id = version_id(&profile, "loader profile")?.to_string();
                    write_loader_profile(&self.minecraft_dir, &profile)?;
                    let merged = self.load_version(&version_id)?;
                    install_version_files_async(&merged, &self.minecraft_dir, reporter).await?;
                    return Ok(InstallResult { version_id });
                }
                LoaderSpec::Quilt { version } => {
                    self.install_vanilla_version_async(&request.minecraft_version, reporter)
                        .await?;
                    let loader_version = resolve_quilt_loader_version_async(version).await?;
                    let profile = crate::loader::quilt::fetch_profile_async(
                        &request.minecraft_version,
                        &loader_version,
                    )
                    .await?;
                    let version_id = version_id(&profile, "loader profile")?.to_string();
                    write_loader_profile(&self.minecraft_dir, &profile)?;
                    let merged = self.load_version(&version_id)?;
                    install_version_files_async(&merged, &self.minecraft_dir, reporter).await?;
                    return Ok(InstallResult { version_id });
                }
                LoaderSpec::Forge { version } => {
                    self.install_vanilla_version_async(&request.minecraft_version, reporter)
                        .await?;
                    let loader_version =
                        resolve_forge_loader_version_async(&request.minecraft_version, version)
                            .await?;
                    let installer_path = download_installer_async(
                        &self.minecraft_dir,
                        "forge",
                        &loader_version,
                        &crate::loader::forge::installer_url(&loader_version),
                    )
                    .await?;
                    let invocation = InstallerInvocation {
                        loader: LoaderKind::Forge,
                        java_executable: PathBuf::from("java"),
                        installer_path,
                        minecraft_dir: self.minecraft_dir.clone(),
                    };
                    tokio::task::spawn_blocking(move || run_loader_installer(&invocation))
                        .await
                        .map_err(|err| LauncherError::Other {
                            message: format!("forge installer worker failed: {err}"),
                        })??;
                    let version_id =
                        crate::loader::forge::forge_installed_version_id(&loader_version)?;
                    let merged = self.load_version(&version_id)?;
                    install_version_files_async(&merged, &self.minecraft_dir, reporter).await?;
                    return Ok(InstallResult { version_id });
                }
                LoaderSpec::NeoForge { version } => {
                    self.install_vanilla_version_async(&request.minecraft_version, reporter)
                        .await?;
                    let loader_version =
                        resolve_neoforge_loader_version_async(&request.minecraft_version, version)
                            .await?;
                    let installer_path = download_installer_async(
                        &self.minecraft_dir,
                        "neoforge",
                        &loader_version,
                        &crate::loader::neoforge::installer_url(&loader_version),
                    )
                    .await?;
                    let invocation = InstallerInvocation {
                        loader: LoaderKind::NeoForge,
                        java_executable: PathBuf::from("java"),
                        installer_path,
                        minecraft_dir: self.minecraft_dir.clone(),
                    };
                    tokio::task::spawn_blocking(move || run_loader_installer(&invocation))
                        .await
                        .map_err(|err| LauncherError::Other {
                            message: format!("neoforge installer worker failed: {err}"),
                        })??;
                    let version_id = crate::loader::neoforge::neoforge_installed_version_id(
                        &request.minecraft_version,
                        &loader_version,
                    );
                    let merged = self.load_version(&version_id)?;
                    install_version_files_async(&merged, &self.minecraft_dir, reporter).await?;
                    return Ok(InstallResult { version_id });
                }
            }
        }

        self.install_vanilla_version_async(&request.minecraft_version, reporter)
            .await?;
        Ok(InstallResult {
            version_id: request.minecraft_version,
        })
    }

    /// Builds a Java launch command from already-loaded version metadata.
    ///
    /// Call [`Launcher::load_version`] after installation to obtain merged
    /// metadata for profiles that inherit from a parent version.
    ///
    /// # Errors
    ///
    /// Returns [`LauncherError`] if required metadata is missing or cannot be
    /// converted into classpath and argument values.
    pub fn build_launch_command_from_version(
        &self,
        version: &VersionJson,
        options: LaunchOptions,
    ) -> Result<LaunchCommand> {
        build_launch_command(version, self.minecraft_dir.clone(), options)
    }

    /// Loads and merges a version JSON from `<minecraft_dir>/versions`.
    ///
    /// If the profile declares `inheritsFrom`, parent metadata is loaded and
    /// merged before the result is returned.
    ///
    /// # Errors
    ///
    /// Returns [`LauncherError`] if the profile or any parent cannot be read or
    /// parsed.
    pub fn load_version(&self, version_id: &str) -> Result<VersionJson> {
        load_version_json(&self.minecraft_dir, version_id)
    }

    fn install_vanilla_version(
        &self,
        version_id: &str,
        reporter: &mut dyn ProgressReporter,
    ) -> Result<()> {
        let version = fetch_vanilla_version(version_id)?;
        write_version_json(&self.minecraft_dir, &version)?;
        install_version_files(&version, &self.minecraft_dir, reporter)
    }

    async fn install_vanilla_version_async(
        &self,
        version_id: &str,
        reporter: &mut dyn ProgressReporter,
    ) -> Result<()> {
        let version = fetch_vanilla_version_async(version_id).await?;
        write_version_json(&self.minecraft_dir, &version)?;
        install_version_files_async(&version, &self.minecraft_dir, reporter).await
    }
}

fn version_id<'a>(version: &'a VersionJson, context: &str) -> Result<&'a str> {
    version
        .id
        .as_deref()
        .ok_or_else(|| LauncherError::MissingField {
            context: context.to_string(),
            field: "id".to_string(),
        })
}
fn resolve_fabric_loader_version(version: LoaderVersion) -> Result<String> {
    match version {
        LoaderVersion::Exact(version) => Ok(version),
        LoaderVersion::Latest | LoaderVersion::LatestStable => {
            let versions = crate::loader::fabric::list_loader_versions()?;
            Ok(crate::loader::fabric::latest_stable_loader(&versions)?
                .version
                .clone())
        }
    }
}

async fn resolve_fabric_loader_version_async(version: LoaderVersion) -> Result<String> {
    match version {
        LoaderVersion::Exact(version) => Ok(version),
        LoaderVersion::Latest | LoaderVersion::LatestStable => {
            let versions = crate::loader::fabric::list_loader_versions_async().await?;
            Ok(crate::loader::fabric::latest_stable_loader(&versions)?
                .version
                .clone())
        }
    }
}

fn resolve_quilt_loader_version(version: LoaderVersion) -> Result<String> {
    match version {
        LoaderVersion::Exact(version) => Ok(version),
        LoaderVersion::Latest | LoaderVersion::LatestStable => {
            let versions = crate::loader::quilt::list_loader_versions()?;
            Ok(crate::loader::quilt::latest_loader(&versions)?
                .version
                .clone())
        }
    }
}

async fn resolve_quilt_loader_version_async(version: LoaderVersion) -> Result<String> {
    match version {
        LoaderVersion::Exact(version) => Ok(version),
        LoaderVersion::Latest | LoaderVersion::LatestStable => {
            let versions = crate::loader::quilt::list_loader_versions_async().await?;
            Ok(crate::loader::quilt::latest_loader(&versions)?
                .version
                .clone())
        }
    }
}

fn resolve_forge_loader_version(minecraft_version: &str, version: LoaderVersion) -> Result<String> {
    match version {
        LoaderVersion::Exact(version) => Ok(version),
        LoaderVersion::Latest | LoaderVersion::LatestStable => {
            let versions = crate::loader::forge::list_forge_versions()?;
            Ok(
                crate::loader::forge::latest_for_minecraft(&versions, minecraft_version)?
                    .to_string(),
            )
        }
    }
}

async fn resolve_forge_loader_version_async(
    minecraft_version: &str,
    version: LoaderVersion,
) -> Result<String> {
    match version {
        LoaderVersion::Exact(version) => Ok(version),
        LoaderVersion::Latest | LoaderVersion::LatestStable => {
            let versions = crate::loader::forge::list_forge_versions_async().await?;
            Ok(
                crate::loader::forge::latest_for_minecraft(&versions, minecraft_version)?
                    .to_string(),
            )
        }
    }
}

fn resolve_neoforge_loader_version(
    minecraft_version: &str,
    version: LoaderVersion,
) -> Result<String> {
    match version {
        LoaderVersion::Exact(version) => Ok(version),
        LoaderVersion::Latest | LoaderVersion::LatestStable => {
            let versions = crate::loader::neoforge::list_neoforge_versions()?;
            Ok(
                crate::loader::neoforge::latest_for_minecraft(&versions, minecraft_version)?
                    .to_string(),
            )
        }
    }
}

async fn resolve_neoforge_loader_version_async(
    minecraft_version: &str,
    version: LoaderVersion,
) -> Result<String> {
    match version {
        LoaderVersion::Exact(version) => Ok(version),
        LoaderVersion::Latest | LoaderVersion::LatestStable => {
            let versions = crate::loader::neoforge::list_neoforge_versions_async().await?;
            Ok(
                crate::loader::neoforge::latest_for_minecraft(&versions, minecraft_version)?
                    .to_string(),
            )
        }
    }
}

fn download_installer(
    minecraft_dir: &Path,
    loader_name: &str,
    loader_version: &str,
    url: &str,
) -> Result<PathBuf> {
    let destination = minecraft_dir
        .join("versions")
        .join(".installers")
        .join(format!("{loader_name}-{loader_version}-installer.jar"));
    let plan = DownloadPlan {
        tasks: vec![DownloadTask {
            url: url.to_string(),
            destination: destination.clone(),
            checksum: None,
            label: format!("{loader_name} installer {loader_version}"),
        }],
    };
    let mut reporter = |_event: ProgressEvent| {};
    execute_plan(&plan, &mut reporter)?;
    Ok(destination)
}

async fn download_installer_async(
    minecraft_dir: &Path,
    loader_name: &str,
    loader_version: &str,
    url: &str,
) -> Result<PathBuf> {
    let destination = minecraft_dir
        .join("versions")
        .join(".installers")
        .join(format!("{loader_name}-{loader_version}-installer.jar"));
    let plan = DownloadPlan {
        tasks: vec![DownloadTask {
            url: url.to_string(),
            destination: destination.clone(),
            checksum: None,
            label: format!("{loader_name} installer {loader_version}"),
        }],
    };
    let mut reporter = |_event: ProgressEvent| {};
    execute_plan_async(&plan, DEFAULT_DOWNLOAD_WORKERS, &mut reporter).await?;
    Ok(destination)
}
