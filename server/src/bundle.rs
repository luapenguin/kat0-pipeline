// SPDX-License-Identifier: AGPL-3.0-only

use std::{
    io::{self, Write as _},
    path::Path,
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use kat0_pipeline_protocol::model::Snapshot;

use crate::RunResult;

const TEMPLATE: &str = include_str!("../assets/index.html");
const PANEL_JS: &[u8] = include_bytes!("../assets/panel.js");
const PANEL_WASM: &[u8] = include_bytes!("../assets/panel.wasm");
const PANEL_CSS: &[u8] = include_bytes!("../assets/report.css");

const CSS_SLOT: &str = "__REPORT_CSS__";
const LOADER_SLOT: &str = "<!-- __REPORT_LOADER__ -->";
const PANEL_PREFIX: &str = r#"id="panel" data-live=""#;
const DATA_TAG: &str = r#"<script id="run-data" type="application/json">"#;
const SNAPSHOT_SLOT: &str = "__RUN_SNAPSHOT__";
const SCRIPT_END: &str = "</script>";

pub struct Bundle {
    html: String,
    live_at: usize,
    snapshot_at: usize,
}

impl Bundle {
    /// Builds the self-contained panel bundle from assets embedded at compile time.
    pub fn embedded() -> RunResult<Self> {
        Self::from_assets(TEMPLATE, PANEL_JS, PANEL_WASM, PANEL_CSS)
    }

    pub fn render(&self, snapshot: &Snapshot, live: bool) -> RunResult<String> {
        Ok(self.render_json(&serde_json::to_string(snapshot)?, live))
    }

    pub(crate) fn render_json(&self, json: &str, live: bool) -> String {
        // Prevent JSON values from terminating (or changing the parsing of) the script element.
        let json = json.replace('<', "\\u003c");
        let mut html = String::with_capacity(self.html.len() + json.len());
        html.push_str(&self.html[..self.live_at]);
        html.push_str(if live { "true" } else { "false" });
        html.push_str(&self.html[self.live_at + "false".len()..self.snapshot_at]);
        html.push_str(&json);
        html.push_str(&self.html[self.snapshot_at + SNAPSHOT_SLOT.len()..]);
        html
    }

    /// Publishes a complete offline report at a new path, without replacing a file.
    pub async fn save(&self, snapshot: &Snapshot, path: &Path) -> RunResult {
        let html = self.render(snapshot, false)?;
        let path = path.to_owned();
        tokio::task::spawn_blocking(move || publish_report(&path, &html)).await??;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self::from_assets(TEMPLATE, b"js", b"\0asm", b"css").unwrap()
    }

    fn from_assets(template: &str, js: &[u8], wasm: &[u8], css: &[u8]) -> RunResult<Self> {
        let mut html = template.to_owned();
        replace_unique(
            &mut html,
            CSS_SLOT,
            &data_url("text/css;charset=utf-8", css),
        )?;
        replace_unique(&mut html, LOADER_SLOT, &inline_loader(js, wasm)?)?;

        let live_at =
            unique_offset(&html, &format!(r#"{PANEL_PREFIX}false""#))? + PANEL_PREFIX.len();
        let snapshot_at = unique_offset(&html, &format!("{DATA_TAG}{SNAPSHOT_SLOT}{SCRIPT_END}"))?
            + DATA_TAG.len();
        if live_at + "false".len() > snapshot_at {
            return Err("#panel debe preceder al bloque #run-data".into());
        }
        Ok(Self {
            html,
            live_at,
            snapshot_at,
        })
    }
}

fn publish_report(path: &Path, html: &str) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent)?;
    // Stage beside the final file so publishing never needs a cross-filesystem copy.
    let mut staged = tempfile::Builder::new()
        .prefix(".report-")
        .tempfile_in(parent)?;
    staged.write_all(html.as_bytes())?;
    staged.as_file().sync_all()?;
    staged
        .persist_noclobber(path)
        .map_err(|error| error.error)?;
    Ok(())
}

fn inline_loader(js: &[u8], wasm: &[u8]) -> RunResult<String> {
    let js_url = serde_json::to_string(&data_url("text/javascript", js))?;
    let wasm_url = serde_json::to_string(&data_url("application/wasm", wasm))?;
    Ok(format!(
        r#"<script type="module">import({js_url}).then((mod)=>mod.default({{module_or_path:{wasm_url}}}).then(()=>mod.hydrate()));</script>"#
    ))
}

fn unique_offset(text: &str, marker: &str) -> RunResult<usize> {
    let mut matches = text.match_indices(marker);
    match (matches.next(), matches.next()) {
        (Some((offset, _)), None) => Ok(offset),
        _ => Err(
            format!("Se esperaba una sola aparición de {marker:?} al empaquetar el informe").into(),
        ),
    }
}

fn replace_unique(text: &mut String, marker: &str, replacement: &str) -> RunResult {
    let start = unique_offset(text, marker)?;
    text.replace_range(start..start + marker.len(), replacement);
    Ok(())
}

fn data_url(mime: &str, bytes: &[u8]) -> String {
    let mut url = format!("data:{mime};base64,");
    STANDARD.encode_string(bytes, &mut url);
    url
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use kat0_pipeline_protocol::model::{Column, Definition, Job, JobState, Row, Status};
    use tokio::fs;

    use super::*;

    #[test]
    fn embedded_bundle_is_self_contained_and_renders_valid_loader() {
        let bundle = Bundle::embedded().expect("embedded panel assets must build");
        assert!(bundle.html.contains("data:text/css;charset=utf-8;base64,"));
        assert!(bundle.html.contains("data:text/javascript;base64,"));
        assert!(bundle.html.contains("data:application/wasm;base64,"));
        assert!(bundle.html.contains("mod.default({module_or_path:"));
        assert!(bundle.html.contains("mod.hydrate()"));
        assert!(!bundle.html.contains(r#"rel="modulepreload""#));
        assert!(!bundle.html.contains(r#"rel="preload""#));
    }

    #[test]
    fn embeds_assets_into_template_without_external_preloads() {
        let bundle = Bundle::from_assets(TEMPLATE, b"js", b"\0asm", b"css").unwrap();
        let expected_loader = concat!(
            r#"<script type="module">import("data:text/javascript;base64,anM=")"#,
            r#".then((mod)=>mod.default({module_or_path:"data:application/wasm;base64,AGFzbQ=="})"#,
            r#".then(()=>mod.hydrate()));</script>"#
        );
        assert_eq!(
            bundle.html,
            TEMPLATE
                .replace(CSS_SLOT, "data:text/css;charset=utf-8;base64,Y3Nz")
                .replace(LOADER_SLOT, expected_loader)
        );
        assert!(!bundle.html.contains(r#"rel="modulepreload""#));
        assert!(!bundle.html.contains(r#"rel="preload""#));
    }

    #[test]
    fn requires_unique_template_markers_and_snapshot_after_panel() {
        let panel = format!(r#"{PANEL_PREFIX}false""#);
        let data = format!("{DATA_TAG}{SNAPSHOT_SLOT}{SCRIPT_END}");
        let assemble = |template: &str| Bundle::from_assets(template, b"", b"", b"");
        for marker in [CSS_SLOT, LOADER_SLOT, panel.as_str(), data.as_str()] {
            assert!(assemble(&TEMPLATE.replace(marker, "")).is_err(), "{marker}");
            let duplicate = TEMPLATE.replace(marker, &marker.repeat(2));
            assert!(assemble(&duplicate).is_err(), "{marker}");
        }
        let reversed = format!("{data}{}", TEMPLATE.replace(data.as_str(), ""));
        assert!(assemble(&reversed).is_err());
    }

    #[test]
    fn snapshot_round_trips_without_leaving_the_json_element() {
        let bundle = Bundle::from_assets(TEMPLATE, b"", b"", b"").unwrap();
        let snapshot = serde_json::json!({"title": "</ScRiPt><!-- café __RUN_SNAPSHOT__"});
        let offline = bundle.render_json(&snapshot.to_string(), false);
        let live = bundle.render_json(&snapshot.to_string(), true);
        assert_eq!(
            live,
            offline.replacen(r#"data-live="false""#, r#"data-live="true""#, 1)
        );
        let (_, script) = offline.split_once(DATA_TAG).unwrap();
        let (json, _) = script.split_once(SCRIPT_END).unwrap();
        assert!(!json.contains('<'));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(json).unwrap(),
            snapshot
        );
    }

    fn snapshot(title: &str) -> Snapshot {
        Snapshot::new(Definition {
            title: title.into(),
            description: String::new(),
            columns: vec![Column {
                title: "Verify".into(),
                jobs: vec![Job {
                    id: "check".into(),
                    title: "Check".into(),
                    is_test: true,
                    dependencies: vec![],
                    steps: vec![],
                    cases: vec![],
                }],
            }],
        })
        .unwrap()
    }

    async fn assert_only_report_in(dir: &Path, report: &Path) {
        let mut entries = fs::read_dir(dir).await.unwrap();
        assert_eq!(entries.next_entry().await.unwrap().unwrap().path(), report);
        assert!(entries.next_entry().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn publishes_complete_offline_html_for_a_failed_execution() {
        let bundle = Bundle::from_assets(TEMPLATE, b"js", b"\0asm", b"css").unwrap();
        let definition = snapshot("Failed run").definition().clone();
        let snapshot = Snapshot::from_parts(
            definition,
            Status::Failed,
            0,
            BTreeMap::from([(
                "check".into(),
                JobState {
                    row: Row {
                        status: Status::Failed,
                        started_ms: Some(0),
                        duration_ms: Some(0),
                    },
                    steps: BTreeMap::new(),
                    cases: BTreeMap::new(),
                },
            )]),
        )
        .unwrap();
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("run");
        let path = dir.join("index.html");
        assert!(!path.exists());

        bundle.save(&snapshot, &path).await.unwrap();

        assert_eq!(
            fs::read_to_string(&path).await.unwrap(),
            bundle.render(&snapshot, false).unwrap()
        );
        assert_only_report_in(&dir, &path).await;
    }

    #[tokio::test]
    async fn publication_leaves_an_existing_report_untouched_and_removes_staging() {
        let bundle = Bundle::from_assets(TEMPLATE, b"", b"", b"").unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index.html");
        fs::write(&path, b"previous report").await.unwrap();

        let error = bundle
            .save(&snapshot("Second run"), &path)
            .await
            .expect_err("path already published");

        assert_eq!(
            error.downcast_ref::<io::Error>().unwrap().kind(),
            io::ErrorKind::AlreadyExists
        );
        assert_eq!(fs::read(&path).await.unwrap(), b"previous report");
        assert_only_report_in(dir.path(), &path).await;
    }

    #[tokio::test]
    async fn concurrent_publishers_leave_one_complete_report() {
        let bundle = Bundle::from_assets(TEMPLATE, b"", b"", b"").unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index.html");
        let (first, second) = (snapshot("First"), snapshot("Second"));

        let results = tokio::join!(bundle.save(&first, &path), bundle.save(&second, &path));

        let (published, rejected) = match results {
            (Ok(()), Err(error)) => (&first, error),
            (Err(error), Ok(())) => (&second, error),
            other => panic!("expected exactly one publication: {other:?}"),
        };
        assert_eq!(
            rejected.downcast_ref::<io::Error>().unwrap().kind(),
            io::ErrorKind::AlreadyExists
        );
        assert_eq!(
            fs::read_to_string(&path).await.unwrap(),
            bundle.render(published, false).unwrap()
        );
        assert_only_report_in(dir.path(), &path).await;
    }
}
