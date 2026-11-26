// SPDX-License-Identifier: AGPL-3.0-only

use kat0_pipeline::{
    RunResult, bundle::Bundle, prepare::reserve_report, protocol::model::Snapshot, wire,
};

use super::publication::{finish_report, report_root};

fn snapshot() -> RunResult<Snapshot> {
    wire::decode(include_bytes!("../../fixtures/web_regression.json"))
}

pub(super) async fn report() -> RunResult {
    let snapshot = snapshot()?;
    let path = reserve_report(&report_root()).await?;
    let bundle = Bundle::embedded()?;
    let saved = bundle.save(&snapshot, &path).await;
    finish_report(
        Vec::new(),
        &path,
        saved,
        "Sample report (no tests executed)",
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use kat0_pipeline::protocol::model::Status;

    #[test]
    fn regression_snapshot_covers_all_job_states() {
        let sample = snapshot().expect("web regression preview fixture");
        let definition = sample.definition();
        Snapshot::new(definition.clone()).expect("valid flow definition");
        assert_eq!(sample.status, Status::Running);

        let expected: [(&str, Status, &[&str]); 8] = [
            ("prepare-env", Status::Passed, &[]),
            ("change-role", Status::Failed, &["prepare-env"]),
            ("sign-in", Status::Passed, &["prepare-env"]),
            ("browse-orgs", Status::Running, &["prepare-env"]),
            ("role-persistence", Status::Skipped, &["change-role"]),
            (
                "save-evidence",
                Status::Pending,
                &["sign-in", "browse-orgs"],
            ),
            (
                "cleanup-env",
                Status::Pending,
                &["role-persistence", "save-evidence"],
            ),
            ("collect-results", Status::Pending, &["cleanup-env"]),
        ];
        assert_eq!(definition.jobs().count(), expected.len());
        for (job, (id, state, dependencies)) in definition.jobs().zip(expected) {
            assert_eq!(job.id, id);
            assert_eq!(job.dependencies.as_slice(), dependencies, "{id}");
            assert_eq!(sample.job_status(id), Some(state), "{id}");
            for step in &job.steps {
                assert!(
                    sample.row(&job.id, Some(&step.id)).is_some(),
                    "{} / {}",
                    job.id,
                    step.id
                );
            }
        }
        let navigation = sample.row("browse-orgs", None).expect("navigation job");
        assert_eq!(navigation.duration_at(sample.elapsed_ms), Some(8_300));
    }
}
