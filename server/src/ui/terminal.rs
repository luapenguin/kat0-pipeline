// SPDX-License-Identifier: AGPL-3.0-only

use std::{
    collections::HashMap,
    env,
    io::{self, IsTerminal as _, Write},
    path::Path,
};
#[cfg(unix)]
use std::{
    fs::File,
    process::{Command, Stdio},
};

use kat0_pipeline_protocol::{
    model::{Job, Snapshot, Status},
    v3::{Event, RunState, TerminalStatus},
};

const BOLD: &str = "\x1b[1m";
const CYAN: &str = "\x1b[36m";
const GREEN: &str = "\x1b[32m";
const RED: &str = "\x1b[31m";
const DIM: &str = "\x1b[2m";
const RESET: &str = "\x1b[0m";
const CLEAR_LINE: &str = "\r\x1b[K";

const DEFAULT_TTY_WIDTH: usize = 100;
const MIN_TTY_WIDTH: usize = 56;
const MAX_JOB_COL_WIDTH: usize = 28;
const DURATION_COL_WIDTH: usize = 8;

/// Stage-grouped, column-aligned live terminal presenter.
/// Uses single-line in-place ANSI updates when attached to a TTY (preventing line-wrap
/// corruption on `\r\x1b[K`) and untruncated plain aligned lines in CI/redirected logs.
pub struct TerminalPresenter<W: Write = io::Stdout> {
    writer: W,
    is_tty: bool,
    width: usize,
    next_number: usize,
    number_width: usize,
    job_col_width: usize,
    job_stages: HashMap<String, String>,
    current_stage: Option<String>,
    item_numbers: HashMap<(String, Option<String>), usize>,
    active_line: bool,
}

impl TerminalPresenter<io::Stdout> {
    pub fn stdout() -> Self {
        let stdout = io::stdout();
        let is_tty = stdout.is_terminal();
        let width = if is_tty {
            detect_terminal_width()
        } else {
            DEFAULT_TTY_WIDTH
        };
        Self::with_width(stdout, is_tty, width)
    }
}

impl<W: Write> TerminalPresenter<W> {
    pub fn with_width(writer: W, is_tty: bool, width: usize) -> Self {
        Self {
            writer,
            is_tty,
            width: width.max(MIN_TTY_WIDTH),
            next_number: 1,
            number_width: 2,
            job_col_width: 16,
            job_stages: HashMap::new(),
            current_stage: None,
            item_numbers: HashMap::new(),
            active_line: false,
        }
    }

    pub fn on_start(&mut self, state: &RunState) {
        let definition = state.snapshot().definition();
        let tests = definition.test_case_count();
        let workers = state.workers();
        self.number_width = tests.max(1).to_string().len().max(2);
        self.job_col_width = definition
            .jobs()
            .filter(|job| job.is_test)
            .map(|job| job.title.chars().count())
            .max()
            .unwrap_or(16)
            .clamp(12, MAX_JOB_COL_WIDTH);

        self.job_stages.clear();
        for column in &definition.columns {
            for job in &column.jobs {
                self.job_stages.insert(job.id.clone(), column.title.clone());
            }
        }

        let test_word = if tests == 1 { "test" } else { "tests" };
        let worker_word = if workers == 1 { "worker" } else { "workers" };
        let _ = writeln!(
            self.writer,
            "\nRunning {tests} {test_word} using {workers} {worker_word}"
        );
        let _ = self.writer.flush();
    }

    pub fn on_event(&mut self, event: &Event, state: &RunState) {
        let snapshot = state.snapshot();
        match event {
            Event::JobStarted { job_id, .. } => {
                if let Some(job) = find_job(snapshot, job_id) {
                    if job.is_test && job.cases.is_empty() {
                        let number = self.number_for(job_id, None);
                        self.ensure_stage(job_id);
                        self.show_active(Some(number), &job.title, None);
                    } else if !job.is_test {
                        self.ensure_stage(job_id);
                        self.show_active(None, &job.title, None);
                    }
                }
            }
            Event::CaseStarted {
                job_id, case_id, ..
            } => {
                if let Some((job_title, case_title)) = case_parts(snapshot, job_id, case_id) {
                    let number = self.number_for(job_id, Some(case_id));
                    self.ensure_stage(job_id);
                    self.show_active(Some(number), job_title, Some(case_title));
                }
            }
            Event::CaseFinished {
                job_id,
                case_id,
                status,
                ..
            } => {
                if let Some((job_title, case_title)) = case_parts(snapshot, job_id, case_id) {
                    let number = self.number_for(job_id, Some(case_id));
                    let duration = snapshot
                        .case_row(job_id, case_id, None)
                        .and_then(|row| row.duration_ms);
                    self.ensure_stage(job_id);
                    self.print_completed(
                        Some(number),
                        job_title,
                        Some(case_title),
                        *status,
                        duration,
                    );
                }
            }
            Event::JobFinished { job_id, status, .. } => {
                if let Some(job) = find_job(snapshot, job_id) {
                    let duration = snapshot.row(job_id, None).and_then(|row| row.duration_ms);
                    if job.is_test && job.cases.is_empty() {
                        let number = self.number_for(job_id, None);
                        self.ensure_stage(job_id);
                        self.print_completed(Some(number), &job.title, None, *status, duration);
                    } else if !job.is_test {
                        self.ensure_stage(job_id);
                        self.print_completed(None, &job.title, None, *status, duration);
                    }
                }
            }
            _ => {}
        }
    }

    pub fn on_finish(&mut self, state: &RunState, report_path: Option<&Path>) {
        self.clear_active_if_needed();
        let snapshot = state.snapshot();
        let (mut passed, mut failed, mut skipped) = (0usize, 0usize, 0usize);
        for job in snapshot.definition().jobs().filter(|job| job.is_test) {
            if job.cases.is_empty() {
                count_status(
                    snapshot.job_status(&job.id),
                    &mut passed,
                    &mut failed,
                    &mut skipped,
                );
            } else {
                for case in &job.cases {
                    let status = snapshot
                        .case_row(&job.id, &case.id, None)
                        .map(|row| row.status);
                    count_status(status, &mut passed, &mut failed, &mut skipped);
                }
            }
        }
        let elapsed = format_duration(snapshot.elapsed_ms);
        let _ = writeln!(self.writer);
        if self.is_tty {
            let _ = writeln!(
                self.writer,
                "  {GREEN}{passed} passed{RESET}, {RED}{failed} failed{RESET}, {DIM}{skipped} skipped{RESET} ({elapsed})"
            );
        } else {
            let _ = writeln!(
                self.writer,
                "  {passed} passed, {failed} failed, {skipped} skipped ({elapsed})"
            );
        }
        if let Some(path) = report_path {
            let _ = writeln!(self.writer, "  Report: {}", path.display());
        }
        let _ = self.writer.flush();
    }

    fn ensure_stage(&mut self, job_id: &str) {
        let Some(stage) = self.job_stages.get(job_id) else {
            return;
        };
        if self.current_stage.as_deref() == Some(stage.as_str()) {
            return;
        }
        let stage = stage.clone();
        self.clear_active_if_needed();
        if self.is_tty {
            let _ = writeln!(self.writer, "\n  {BOLD}{CYAN}{stage}{RESET}");
        } else {
            let _ = writeln!(self.writer, "\n  {stage}");
        }
        self.current_stage = Some(stage);
    }

    fn number_for(&mut self, job_id: &str, case_id: Option<&str>) -> usize {
        let key = (job_id.to_owned(), case_id.map(str::to_owned));
        if let Some(&existing) = self.item_numbers.get(&key) {
            existing
        } else {
            let assigned = self.next_number;
            self.next_number += 1;
            self.item_numbers.insert(key, assigned);
            assigned
        }
    }

    fn show_active(&mut self, number: Option<usize>, job_title: &str, case_title: Option<&str>) {
        if !self.is_tty {
            return;
        }
        self.clear_active_if_needed();
        let line = self.format_row(RowSpec {
            symbol: "…",
            symbol_color: DIM,
            number,
            job_title,
            case_title,
            meta: "running",
        });
        let _ = write!(self.writer, "{line}");
        let _ = self.writer.flush();
        self.active_line = true;
    }

    fn print_completed(
        &mut self,
        number: Option<usize>,
        job_title: &str,
        case_title: Option<&str>,
        status: TerminalStatus,
        duration_ms: Option<u64>,
    ) {
        self.clear_active_if_needed();
        let (symbol, color) = match status {
            TerminalStatus::Passed => ("✓", GREEN),
            TerminalStatus::Failed => ("✘", RED),
            TerminalStatus::Skipped | _ => ("-", DIM),
        };
        let meta = match (status, duration_ms) {
            (TerminalStatus::Skipped, _) => "skipped".to_owned(),
            (_, Some(ms)) => format_duration(ms),
            (_, None) => "—".to_owned(),
        };
        let line = self.format_row(RowSpec {
            symbol,
            symbol_color: color,
            number,
            job_title,
            case_title,
            meta: &meta,
        });
        let _ = writeln!(self.writer, "{line}");
        let _ = self.writer.flush();
    }

    fn format_row(&self, row: RowSpec<'_>) -> String {
        let num = match row.number {
            Some(n) => format!("{n:>width$}", width = self.number_width),
            None => format!("{:>width$}", "·", width = self.number_width),
        };
        // Keep one spare column on TTYs so terminals never wrap onto a second physical line.
        let max_line = self.width.saturating_sub(1);
        // Layout: "    " (4) + symbol (1) + "  " (2) + num (number_width) + "  " (2) = 9 + number_width.
        let prefix_len = 9 + self.number_width;
        let meta_width = DURATION_COL_WIDTH.max(row.meta.chars().count());
        let body_budget = max_line.saturating_sub(prefix_len + 2 + meta_width);

        let body = if row.number.is_some() {
            let job_col = self.job_col_width.min(body_budget.max(1));
            let job_cell = pad_or_truncate(row.job_title, job_col);
            match row.case_title {
                Some(case_text) => {
                    let detail_budget = body_budget.saturating_sub(job_col + 2);
                    let detail = if self.is_tty {
                        pad_or_truncate(case_text, detail_budget)
                    } else {
                        case_text.to_owned()
                    };
                    if detail.is_empty() {
                        job_cell
                    } else {
                        format!("{job_cell}  {detail}")
                    }
                }
                None => {
                    if self.is_tty {
                        pad_or_truncate(&job_cell, body_budget)
                    } else {
                        job_cell
                    }
                }
            }
        } else if self.is_tty {
            pad_or_truncate(row.job_title, body_budget)
        } else {
            pad_or_truncate(
                row.job_title,
                self.job_col_width.max(row.job_title.chars().count()),
            )
        };

        let meta_cell = format!("{:>meta_width$}", row.meta);
        if self.is_tty {
            let body_color = if row.symbol == "-" { DIM } else { "" };
            let body_reset = if row.symbol == "-" { RESET } else { "" };
            format!(
                "    {}{}{RESET}  {DIM}{num}{RESET}  {body_color}{body}{body_reset}  {DIM}{meta_cell}{RESET}",
                row.symbol_color, row.symbol
            )
        } else {
            format!("    {}  {num}  {body}  {meta_cell}", row.symbol)
        }
    }

    fn clear_active_if_needed(&mut self) {
        if self.active_line {
            let _ = write!(self.writer, "{CLEAR_LINE}");
            self.active_line = false;
        }
    }

    #[cfg(test)]
    fn into_inner(self) -> W {
        self.writer
    }
}

struct RowSpec<'a> {
    symbol: &'a str,
    symbol_color: &'a str,
    number: Option<usize>,
    job_title: &'a str,
    case_title: Option<&'a str>,
    meta: &'a str,
}

fn detect_terminal_width() -> usize {
    if let Ok(raw) = env::var("COLUMNS")
        && let Ok(cols) = raw.trim().parse::<usize>()
        && cols >= MIN_TTY_WIDTH
    {
        return cols;
    }
    #[cfg(unix)]
    if let Ok(tty) = File::open("/dev/tty")
        && let Ok(output) = Command::new("stty")
            .arg("size")
            .stdin(Stdio::from(tty))
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()
        && output.status.success()
        && let Ok(text) = std::str::from_utf8(&output.stdout)
        && let Some(cols) = text
            .split_whitespace()
            .nth(1)
            .and_then(|value| value.parse::<usize>().ok())
        && cols >= MIN_TTY_WIDTH
    {
        return cols;
    }
    DEFAULT_TTY_WIDTH
}

fn pad_or_truncate(text: &str, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    let count = text.chars().count();
    if count <= width {
        let mut out = String::with_capacity(text.len() + (width - count));
        out.push_str(text);
        for _ in count..width {
            out.push(' ');
        }
        out
    } else if width == 1 {
        "…".to_owned()
    } else {
        let mut out: String = text.chars().take(width - 1).collect();
        out.push('…');
        out
    }
}

fn find_job<'a>(snapshot: &'a Snapshot, job_id: &str) -> Option<&'a Job> {
    snapshot.definition().jobs().find(|job| job.id == job_id)
}

fn case_parts<'a>(
    snapshot: &'a Snapshot,
    job_id: &str,
    case_id: &str,
) -> Option<(&'a str, &'a str)> {
    let job = find_job(snapshot, job_id)?;
    let case = job.cases.iter().find(|case| case.id == case_id)?;
    Some((job.title.as_str(), case.title.as_str()))
}

fn count_status(
    status: Option<Status>,
    passed: &mut usize,
    failed: &mut usize,
    skipped: &mut usize,
) {
    match status {
        Some(Status::Passed) => *passed += 1,
        Some(Status::Failed) => *failed += 1,
        Some(Status::Skipped | Status::Pending | Status::Running | _) | None => {
            *skipped += 1;
        }
    }
}

pub fn format_duration(ms: u64) -> String {
    if ms < 1_000 {
        format!("{ms}ms")
    } else if ms < 60_000 {
        format!("{:.1}s", ms as f64 / 1_000.0)
    } else {
        let minutes = ms / 60_000;
        let seconds = (ms % 60_000) as f64 / 1_000.0;
        format!("{minutes}m {seconds:.1}s")
    }
}

#[cfg(test)]
mod tests {
    use kat0_pipeline_protocol::v3::decode_line;

    use super::*;

    const FIXTURE_V3: &str = include_str!("../../fixtures/web_regression_v3.ndjson");

    fn render_fixture(is_tty: bool, width: usize) -> String {
        let mut lines = FIXTURE_V3.lines().filter(|line| !line.trim().is_empty());
        let first = decode_line(lines.next().unwrap()).unwrap();
        let mut state = RunState::start(first).unwrap();
        let mut presenter = TerminalPresenter::with_width(Vec::new(), is_tty, width);
        presenter.on_start(&state);
        for line in lines {
            let event = decode_line(line).unwrap();
            state.apply(event.clone()).unwrap();
            presenter.on_event(&event, &state);
        }
        presenter.on_finish(&state, Some(Path::new("/tmp/run-1/index.html")));
        String::from_utf8(presenter.into_inner()).unwrap()
    }

    fn strip_ansi(input: &str) -> String {
        let mut out = String::with_capacity(input.len());
        let mut chars = input.chars().peekable();
        while let Some(ch) = chars.next() {
            if ch == '\x1b' && chars.peek() == Some(&'[') {
                chars.next();
                for next in chars.by_ref() {
                    if next.is_ascii_alphabetic() {
                        break;
                    }
                }
            } else {
                out.push(ch);
            }
        }
        out
    }

    #[test]
    fn plain_ci_output_is_stage_grouped_column_aligned_and_ansi_free() {
        let output = render_fixture(false, 100);
        assert!(!output.contains('\x1b'));
        assert!(output.contains("Running 3 tests using 1 worker"));
        assert!(output.contains("\n  Prepare\n"));
        assert!(output.contains("\n  Regression\n"));
        assert!(output.contains("\n  Cleanup\n"));
        assert!(output.contains(
            "    ✓   1  Functionality      Create user and assign to team with reload persistence      1.5s"
        ));
        assert!(output.contains(
            "    ✘   2  Errors & recovery  Controlled DNS write races preserve draft and commit real retries      1.4s"
        ));
        assert!(output.contains(
            "    -   3  Errors & recovery  Controlled Menu Builder writes keep draft and ignore obsolete responses   skipped"
        ));
        assert!(output.contains("  1 passed, 1 failed, 1 skipped (6.4s)"));
        assert!(output.contains("  Report: /tmp/run-1/index.html"));
    }

    #[test]
    fn tty_output_truncates_rows_to_terminal_width_to_prevent_in_place_wrapping() {
        let width = 80;
        let output = render_fixture(true, width);
        assert!(output.contains(CLEAR_LINE));
        assert!(output.contains(GREEN));
        assert!(output.contains(RED));
        for segment in output.split('\r') {
            for line in segment.lines() {
                let plain = strip_ansi(line);
                if plain.starts_with("    ") {
                    assert!(
                        plain.chars().count() < width,
                        "TTY row exceeded width {width}: {plain:?} (len={})",
                        plain.chars().count()
                    );
                }
            }
        }
    }
}
