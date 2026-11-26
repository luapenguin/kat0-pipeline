// SPDX-License-Identifier: AGPL-3.0-only

use std::{
    ffi::OsString,
    path::{Path, PathBuf},
};

use kat0_pipeline::{
    RunResult,
    bundle::Bundle,
    import,
    validate::{self, Failure},
};

pub(super) async fn import_report(input: &Path) -> RunResult {
    let imported = import::prepare(input, &report_root()).await?;
    let saved = async {
        Bundle::embedded()?
            .save(&imported.snapshot, &imported.report_path)
            .await
    }
    .await;
    finish_report(
        imported.failures,
        &imported.report_path,
        saved,
        "Imported report",
    )
    .await
}

pub(super) async fn finish_report(
    failures: Vec<Failure>,
    path: &Path,
    saved: RunResult,
    label: &str,
) -> RunResult {
    let validation = validate::run(failures, path, saved).await;
    if let Some(path) = validation.report_path {
        println!("{label}: {}", path.display());
    }
    validation.result?;
    Ok(())
}

pub(super) fn report_root() -> PathBuf {
    resolve_report_root(
        std::env::var_os("PIPELINE_REPORT_ROOT"),
        std::env::current_dir().ok(),
        std::env::temp_dir(),
    )
}

fn resolve_report_root(
    env_override: Option<OsString>,
    cwd: Option<PathBuf>,
    temp_dir: PathBuf,
) -> PathBuf {
    if let Some(root) = env_override.filter(|value| !value.is_empty()) {
        return PathBuf::from(root);
    }
    cwd.map(|dir| dir.join("target/pipeline"))
        .unwrap_or_else(|| temp_dir.join("kat0-pipeline"))
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    #[test]
    fn report_root_prefers_env_then_cwd_then_temp_dir() {
        let cwd = PathBuf::from("/work/project");
        let tmp = PathBuf::from("/tmp");

        assert_eq!(
            resolve_report_root(
                Some(OsString::from("/custom/reports")),
                Some(cwd.clone()),
                tmp.clone()
            ),
            PathBuf::from("/custom/reports")
        );
        assert_eq!(
            resolve_report_root(Some(OsString::new()), Some(cwd.clone()), tmp.clone()),
            PathBuf::from("/work/project/target/pipeline")
        );
        assert_eq!(
            resolve_report_root(None, Some(cwd), tmp.clone()),
            PathBuf::from("/work/project/target/pipeline")
        );
        assert_eq!(
            resolve_report_root(None, None, tmp),
            PathBuf::from("/tmp/kat0-pipeline")
        );
    }

    #[tokio::test]
    async fn shows_execution_and_report_write_errors_together() {
        let error = finish_report(
            vec![Failure {
                kind: validate::FailureKind::Execution,
                site: validate::FailureSite::Run,
                cause: "job failed".into(),
            }],
            Path::new("run-demo/index.html"),
            Err(io::Error::new(io::ErrorKind::PermissionDenied, "write denied").into()),
            "Report",
        )
        .await
        .expect_err("execution and save failed");
        let display = error.to_string();
        assert!(display.contains("Fallo de ejecución: job failed"));
        assert!(display.contains("Fallo al guardar el informe: run-demo/index.html: write denied"));
    }
}
