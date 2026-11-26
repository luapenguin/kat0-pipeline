// SPDX-License-Identifier: AGPL-3.0-only

//! Local pre-push CI runner mirroring the 6 verification jobs in `.github/workflows/ci.yml`.

use std::{
    error::Error,
    ffi::OsStr,
    fmt, fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant},
};

use crate::panel;

type TaskResult<T = ()> = Result<T, Box<dyn Error>>;

const MSRV_CORE: &str = "1.88.0";
const MSRV_PLAYWRIGHT: &str = "1.95.0";
const MAX_CRATE_BYTES: u64 = 10 * 1024 * 1024;

const PUBLISHABLE_CRATES: [&str; 4] = [
    "kat0-pipeline-protocol",
    "kat0-pipeline-reporter",
    "kat0-pipeline-playwright",
    "kat0-pipeline",
];

const HOST_CRATES: [&str; 6] = [
    "kat0-pipeline-protocol",
    "kat0-pipeline-reporter",
    "kat0-pipeline-playwright",
    "kat0-pipeline",
    "xtask",
    "kat0-pipeline-e2e",
];

const DOC_CRATES: [&str; 3] = [
    "kat0-pipeline-protocol",
    "kat0-pipeline-reporter",
    "kat0-pipeline-playwright",
];

const CORE_MSRV_CRATES: [&str; 3] = [
    "kat0-pipeline-protocol",
    "kat0-pipeline-reporter",
    "kat0-pipeline",
];

/// Individual local CI checks matching `.github/workflows/ci.yml`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Check {
    LintAndUnit,
    MsrvCore,
    MsrvPlaywright,
    PackageAndInstall,
    AssetsReproducibility,
    E2eFirefox,
}

impl Check {
    /// All 6 pre-push checks in canonical execution order.
    pub const ALL: [Self; 6] = [
        Self::LintAndUnit,
        Self::MsrvCore,
        Self::MsrvPlaywright,
        Self::PackageAndInstall,
        Self::AssetsReproducibility,
        Self::E2eFirefox,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::LintAndUnit => "lint-and-unit",
            Self::MsrvCore => "msrv-core",
            Self::MsrvPlaywright => "msrv-playwright",
            Self::PackageAndInstall => "package-and-install",
            Self::AssetsReproducibility => "assets-reproducibility",
            Self::E2eFirefox => "e2e-firefox",
        }
    }

    pub fn description(self) -> &'static str {
        match self {
            Self::LintAndUnit => {
                "cargo fmt, clippy (host + WASM), rustdoc, unit/doc tests, and asset manifest"
            }
            Self::MsrvCore => {
                "MSRV Rust 1.88 on core publishable crates (protocol, reporter, server)"
            }
            Self::MsrvPlaywright => "MSRV Rust 1.95 on kat0-pipeline-playwright",
            Self::PackageAndInstall => {
                "cargo package (<10 MiB, LICENSE + README) and clean cargo install in temp dir"
            }
            Self::AssetsReproducibility => {
                "reproducible WASM panel rebuild and SHA-256 verification via cargo-leptos"
            }
            Self::E2eFirefox => "Playwright Firefox E2E suite (pipeline_viewer) against built CLI",
        }
    }

    fn parse(raw: &str) -> Result<Self, String> {
        Self::ALL
            .into_iter()
            .find(|check| check.name() == raw)
            .ok_or_else(|| {
                let valid = Self::ALL
                    .iter()
                    .map(|check| check.name())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("Unknown CI check {raw:?}. Valid checks: {valid}")
            })
    }
}

impl fmt::Display for Check {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}

/// Parsed action for `xtask ci`.
#[derive(Debug, PartialEq, Eq)]
pub enum CiAction {
    List,
    Run(Vec<Check>),
}

/// Parses arguments passed after `ci`.
pub fn parse_args(args: &[&str]) -> TaskResult<CiAction> {
    if args.is_empty() {
        return Ok(CiAction::Run(Check::ALL.to_vec()));
    }
    if args == ["--list"] {
        return Ok(CiAction::List);
    }

    let mut selected = Vec::new();
    let mut index = 0;
    while index < args.len() {
        match args[index] {
            "--only" => {
                let raw = args
                    .get(index + 1)
                    .ok_or("Missing check name after --only (use `ci --list` to inspect names)")?;
                for item in raw.split(',').filter(|part| !part.is_empty()) {
                    let check = Check::parse(item)?;
                    if !selected.contains(&check) {
                        selected.push(check);
                    }
                }
                index += 2;
            }
            other => {
                return Err(format!(
                    "Unknown argument {other:?} for `xtask ci`. Use `ci [--list | --only <name>[,<name>...]]`."
                )
                .into());
            }
        }
    }

    if selected.is_empty() {
        return Err("No checks selected after --only".into());
    }

    // Preserve canonical execution order regardless of flag ordering.
    let ordered = Check::ALL
        .into_iter()
        .filter(|check| selected.contains(check))
        .collect();
    Ok(CiAction::Run(ordered))
}

/// Executes the parsed `ci` action.
pub fn execute(action: CiAction) -> TaskResult {
    match action {
        CiAction::List => {
            println!("Available local pre-push CI checks ({}):", Check::ALL.len());
            for (index, check) in Check::ALL.iter().enumerate() {
                println!(
                    "  {}. {:24} {}",
                    index + 1,
                    check.name(),
                    check.description()
                );
            }
            Ok(())
        }
        CiAction::Run(checks) => run_checks(&checks),
    }
}

fn run_checks(checks: &[Check]) -> TaskResult {
    let root = panel::workspace_root();
    let total = checks.len();
    let started = Instant::now();
    let mut summary: Vec<(Check, Duration)> = Vec::with_capacity(total);

    for (index, &check) in checks.iter().enumerate() {
        println!(
            "\n==> [{}/{}] {} — {}",
            index + 1,
            total,
            check.name(),
            check.description()
        );
        let step_start = Instant::now();
        run_single(&root, check)?;
        let elapsed = step_start.elapsed();
        println!("    PASS {} ({:.1}s)", check.name(), elapsed.as_secs_f64());
        summary.push((check, elapsed));
    }

    println!(
        "\nAll {total}/{total} local CI checks passed in {:.1}s:",
        started.elapsed().as_secs_f64()
    );
    for (check, elapsed) in summary {
        println!("  [PASS] {:24} {:.1}s", check.name(), elapsed.as_secs_f64());
    }
    Ok(())
}

fn run_single(root: &Path, check: Check) -> TaskResult {
    match check {
        Check::LintAndUnit => run_lint_and_unit(root),
        Check::MsrvCore => run_msrv(root, MSRV_CORE, &CORE_MSRV_CRATES, false),
        Check::MsrvPlaywright => {
            run_msrv(root, MSRV_PLAYWRIGHT, &["kat0-pipeline-playwright"], true)
        }
        Check::PackageAndInstall => run_package_and_install(root),
        Check::AssetsReproducibility => panel::verify(true),
        Check::E2eFirefox => run_e2e_firefox(root),
    }
}

fn run_lint_and_unit(root: &Path) -> TaskResult {
    let skip_driver = [("PLAYWRIGHT_SKIP_DRIVER_DOWNLOAD", "1")];

    run_cmd(root, "cargo", &["fmt", "--all", "--", "--check"], &[])?;

    let mut host_clippy = vec!["clippy", "--locked"];
    extend_package_flags(&mut host_clippy, &HOST_CRATES);
    host_clippy.extend(["--all-targets", "--", "-D", "warnings"]);
    run_cmd(root, "cargo", &host_clippy, &skip_driver)?;

    run_cmd(
        root,
        "cargo",
        &[
            "clippy",
            "--locked",
            "-p",
            "kat0-pipeline-client",
            "--lib",
            "--target",
            "wasm32-unknown-unknown",
            "--",
            "-D",
            "warnings",
        ],
        &[],
    )?;

    let mut doc_args = vec!["doc", "--locked"];
    extend_package_flags(&mut doc_args, &DOC_CRATES);
    doc_args.push("--no-deps");
    run_cmd(
        root,
        "cargo",
        &doc_args,
        &[
            ("PLAYWRIGHT_SKIP_DRIVER_DOWNLOAD", "1"),
            ("RUSTDOCFLAGS", "-D warnings"),
        ],
    )?;

    let mut test_args = vec!["test", "--locked"];
    extend_package_flags(&mut test_args, &HOST_CRATES);
    run_cmd(root, "cargo", &test_args, &skip_driver)?;

    panel::verify(false)
}

fn run_msrv(
    root: &Path,
    version: &str,
    crates: &[&str],
    skip_playwright_driver: bool,
) -> TaskResult {
    ensure_rustup_toolchain(version)?;
    let toolchain = format!("+{version}");
    let mut args = vec![toolchain.as_str(), "test", "--locked"];
    extend_package_flags(&mut args, crates);
    let env_vars: &[(&str, &str)] = if skip_playwright_driver {
        &[("PLAYWRIGHT_SKIP_DRIVER_DOWNLOAD", "1")]
    } else {
        &[]
    };
    run_cmd(root, "cargo", &args, env_vars)
}

fn run_package_and_install(root: &Path) -> TaskResult {
    for pkg in PUBLISHABLE_CRATES {
        let output = capture_cmd(
            root,
            "cargo",
            &["package", "--list", "--locked", "--allow-dirty", "-p", pkg],
            &[],
        )?;
        verify_packaged_metadata_files(pkg, &output)?;
    }

    let package_dir = root.join("target/package");
    if package_dir.exists() {
        fs::remove_dir_all(&package_dir)?;
    }

    run_cmd(
        root,
        "cargo",
        &[
            "package",
            "--offline",
            "--locked",
            "--allow-dirty",
            "-p",
            "kat0-pipeline-protocol",
        ],
        &[],
    )?;

    let mut multi_pkg = vec![
        "package",
        "--offline",
        "--locked",
        "--allow-dirty",
        "--no-verify",
    ];
    extend_package_flags(&mut multi_pkg, &PUBLISHABLE_CRATES);
    run_cmd(root, "cargo", &multi_pkg, &[])?;

    verify_crate_archive_sizes(&package_dir, PUBLISHABLE_CRATES.len())?;

    let install_root = tempfile::Builder::new()
        .prefix("kat0-install-check-")
        .tempdir()?;
    let install_root_str = path_utf8(install_root.path())?;
    run_cmd(
        root,
        "cargo",
        &[
            "install",
            "--offline",
            "--path",
            "server",
            "--locked",
            "--root",
            install_root_str,
        ],
        &[],
    )?;

    let bin_name = if cfg!(windows) {
        "kat0-pipeline.exe"
    } else {
        "kat0-pipeline"
    };
    let bin = install_root.path().join("bin").join(bin_name);
    let report_out = tempfile::Builder::new()
        .prefix("kat0-sample-check-")
        .tempdir()?;
    let report_out_str = path_utf8(report_out.path())?;
    let cwd = std::env::temp_dir();

    run_cmd(&cwd, &bin, &["--version"], &[])?;
    run_cmd(
        &cwd,
        &bin,
        &["sample"],
        &[
            ("PIPELINE_REPORT_ROOT", report_out_str),
            ("PIPELINE_NO_OPEN", "1"),
        ],
    )?;

    if !contains_file_named(report_out.path(), "index.html")? {
        return Err(format!(
            "Installed kat0-pipeline sample did not generate index.html under {}",
            report_out.path().display()
        )
        .into());
    }
    Ok(())
}

fn run_e2e_firefox(root: &Path) -> TaskResult {
    run_cmd(
        root,
        "cargo",
        &[
            "build",
            "--locked",
            "-p",
            "kat0-pipeline",
            "--bin",
            "kat0-pipeline",
        ],
        &[],
    )?;
    run_cmd(
        root,
        "cargo",
        &[
            "test",
            "--locked",
            "-p",
            "kat0-pipeline-e2e",
            "--test",
            "pipeline_viewer",
            "--",
            "--ignored",
        ],
        &[],
    )
}

fn ensure_rustup_toolchain(version: &str) -> TaskResult {
    let output = Command::new("rustup")
        .args(["toolchain", "list"])
        .stdin(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .map_err(|error| format!("Failed to invoke `rustup toolchain list`: {error}"))?;
    if !output.status.success() {
        return Err(format!("`rustup toolchain list` failed ({})", output.status).into());
    }
    let list = String::from_utf8_lossy(&output.stdout);
    if has_installed_toolchain(&list, version) {
        return Ok(());
    }
    Err(format!(
        "Rust toolchain {version} is not installed locally.\n\
         Install both MSRV toolchains once with:\n  \
         rustup toolchain install {MSRV_CORE} {MSRV_PLAYWRIGHT} --profile minimal"
    )
    .into())
}

fn has_installed_toolchain(toolchain_list: &str, version: &str) -> bool {
    let prefix = format!("{version}-");
    toolchain_list.lines().any(|line| {
        let name = line.split_whitespace().next().unwrap_or_default();
        name == version || name.starts_with(&prefix)
    })
}

fn verify_packaged_metadata_files(pkg: &str, file_list: &str) -> TaskResult {
    for required in ["LICENSE", "README.md"] {
        if !file_list.lines().any(|line| line.trim() == required) {
            return Err(format!("Packaged crate {pkg} is missing required file {required}").into());
        }
    }
    Ok(())
}

fn verify_crate_archive_sizes(package_dir: &Path, expected_count: usize) -> TaskResult {
    let mut archives: Vec<(PathBuf, u64)> = Vec::new();
    for entry in fs::read_dir(package_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(OsStr::to_str) == Some("crate") {
            let size = entry.metadata()?.len();
            if size >= MAX_CRATE_BYTES {
                return Err(format!(
                    "{} ({size} bytes) exceeds crates.io 10 MiB limit",
                    path.display()
                )
                .into());
            }
            archives.push((path, size));
        }
    }
    if archives.len() != expected_count {
        return Err(format!(
            "Expected {expected_count} .crate archives in {}, found {}",
            package_dir.display(),
            archives.len()
        )
        .into());
    }
    archives.sort_by(|left, right| left.0.cmp(&right.0));
    for (path, size) in archives {
        println!(
            "    archive: {} ({:.1} KiB)",
            path.file_name().and_then(OsStr::to_str).unwrap_or_default(),
            size as f64 / 1024.0
        );
    }
    Ok(())
}

fn contains_file_named(dir: &Path, target_name: &str) -> TaskResult<bool> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_file() && entry.file_name() == target_name {
            return Ok(true);
        }
        if file_type.is_dir() && contains_file_named(&entry.path(), target_name)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn extend_package_flags<'a>(args: &mut Vec<&'a str>, crates: &[&'a str]) {
    for pkg in crates {
        args.push("-p");
        args.push(pkg);
    }
}

fn path_utf8(path: &Path) -> TaskResult<&str> {
    path.to_str()
        .ok_or_else(|| format!("Path is not valid UTF-8: {}", path.display()).into())
}

fn run_cmd(
    cwd: &Path,
    program: impl AsRef<OsStr>,
    args: &[&str],
    envs: &[(&str, &str)],
) -> TaskResult {
    let mut command = Command::new(program.as_ref());
    command
        .current_dir(cwd)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    for &(key, value) in envs {
        command.env(key, value);
    }
    let status = command
        .status()
        .map_err(|error| format!("Failed to run {:?} {:?}: {error}", program.as_ref(), args))?;
    if !status.success() {
        return Err(format!(
            "Command {:?} {} failed ({status})",
            program.as_ref(),
            args.join(" ")
        )
        .into());
    }
    Ok(())
}

fn capture_cmd(
    cwd: &Path,
    program: impl AsRef<OsStr>,
    args: &[&str],
    envs: &[(&str, &str)],
) -> TaskResult<String> {
    let mut command = Command::new(program.as_ref());
    command
        .current_dir(cwd)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    for &(key, value) in envs {
        command.env(key, value);
    }
    let output = command
        .output()
        .map_err(|error| format!("Failed to run {:?} {:?}: {error}", program.as_ref(), args))?;
    if !output.status.success() {
        return Err(format!(
            "Command {:?} {} failed ({})",
            program.as_ref(),
            args.join(" "),
            output.status
        )
        .into());
    }
    String::from_utf8(output.stdout).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_default_list_and_selective_ci_checks_in_canonical_order() {
        assert_eq!(parse_args(&[]).unwrap(), CiAction::Run(Check::ALL.to_vec()));
        assert_eq!(parse_args(&["--list"]).unwrap(), CiAction::List);
        assert_eq!(
            parse_args(&["--only", "e2e-firefox,lint-and-unit", "--only", "msrv-core"]).unwrap(),
            CiAction::Run(vec![Check::LintAndUnit, Check::MsrvCore, Check::E2eFirefox,])
        );
        assert!(parse_args(&["--only"]).is_err());
        assert!(parse_args(&["--only", "unknown"]).is_err());
        assert!(parse_args(&["--bogus"]).is_err());
    }

    #[test]
    fn detects_installed_rustup_toolchains_and_packaged_license_files() {
        let list = "stable-x86_64-unknown-linux-gnu (default)\n1.88.0-x86_64-unknown-linux-gnu\n";
        assert!(has_installed_toolchain(list, "1.88.0"));
        assert!(!has_installed_toolchain(list, "1.95.0"));

        verify_packaged_metadata_files("demo", "Cargo.toml\nLICENSE\nREADME.md\nsrc/lib.rs\n")
            .unwrap();
        assert!(verify_packaged_metadata_files("demo", "Cargo.toml\nREADME.md\n").is_err());
        assert!(verify_packaged_metadata_files("demo", "Cargo.toml\nLICENSE\n").is_err());
    }
}
