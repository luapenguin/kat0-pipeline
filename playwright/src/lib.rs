// SPDX-License-Identifier: AGPL-3.0-only

//! Optional `playwright-rs` adapter for [`kat0_pipeline_reporter`].
//!
//! Isolates `playwright-rs` error classification and safe browser/context lifecycle helpers
//! in a dedicated adapter crate so core protocol and reporter crates stay free of browser
//! driver dependencies.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::{error::Error, time::Duration};

use kat0_pipeline_reporter::{
    BoxError, RunResult, TypedFailure, execute, protocol::model::FailureKind,
};
use playwright_rs::{
    Browser, BrowserContext, BrowserContextOptions, Error as PlaywrightError, LaunchOptions, Page,
    Playwright,
};

pub use kat0_pipeline_reporter as reporter;
pub use playwright_rs;

/// Classifies a [`PlaywrightError`] into a protocol [`TypedFailure`] where applicable:
///
/// - [`PlaywrightError::AssertionFailed`] / [`PlaywrightError::AssertionTimeout`] -> [`FailureKind::Assertion`]
/// - [`PlaywrightError::Timeout`] -> [`FailureKind::Timeout`]
/// - [`PlaywrightError::LaunchFailed`] -> [`FailureKind::Startup`]
/// - All other variants remain unwrapped operational errors.
pub fn classify_error(error: PlaywrightError) -> BoxError {
    match error {
        PlaywrightError::AssertionFailed(_) | PlaywrightError::AssertionTimeout(_) => {
            TypedFailure::new(FailureKind::Assertion, error).into()
        }
        PlaywrightError::Timeout(_) => TypedFailure::new(FailureKind::Timeout, error).into(),
        PlaywrightError::LaunchFailed(_) => TypedFailure::new(FailureKind::Startup, error).into(),
        other => other.into(),
    }
}

/// Maps only expectation/assertion failures to [`FailureKind::Assertion`], keeping timeout
/// and protocol errors unwrapped.
pub fn assertion_error(error: PlaywrightError) -> BoxError {
    match error {
        PlaywrightError::AssertionFailed(_) | PlaywrightError::AssertionTimeout(_) => {
            TypedFailure::new(FailureKind::Assertion, error).into()
        }
        other => other.into(),
    }
}

/// Wraps any driver or browser startup error as [`FailureKind::Startup`].
pub fn startup_error(error: impl Error + Send + Sync + 'static) -> BoxError {
    TypedFailure::new(FailureKind::Startup, error).into()
}

/// Owns a Playwright driver and launched Firefox [`Browser`], guaranteeing both are closed.
pub struct FirefoxSession {
    driver: Playwright,
    browser: Browser,
}

impl FirefoxSession {
    /// Starts the Playwright driver and launches Firefox within `launch_timeout`.
    pub async fn launch(options: LaunchOptions, launch_timeout: Duration) -> RunResult<Self> {
        let driver = execute::within(launch_timeout, Playwright::launch())
            .await
            .map_err(|error| TypedFailure::new(FailureKind::Startup, error))?;
        let browser = execute::within(
            launch_timeout,
            Box::pin(driver.firefox().launch_with_options(options)),
        )
        .await;
        match browser {
            Ok(browser) => Ok(Self { driver, browser }),
            Err(error) => {
                let _ = execute::within(launch_timeout, driver.shutdown()).await;
                Err(TypedFailure::new(FailureKind::Startup, error).into())
            }
        }
    }

    /// Returns a reference to the running Firefox [`Browser`].
    pub fn browser(&self) -> &Browser {
        &self.browser
    }

    /// Closes the browser and shuts down the Playwright driver, preserving both errors if any.
    pub async fn close(self, close_timeout: Duration) -> RunResult {
        let closed_browser = execute::within(close_timeout, self.browser.close()).await;
        let stopped_driver = execute::within(close_timeout, self.driver.shutdown()).await;
        execute::combine(closed_browser, stopped_driver)
    }
}

/// Opens an isolated [`BrowserContext`], executes `work`, and guarantees `context.close()`.
pub async fn with_context(
    browser: &Browser,
    options: Option<BrowserContextOptions>,
    open_timeout: Duration,
    close_timeout: Duration,
    work: impl AsyncFnOnce(&BrowserContext) -> RunResult,
) -> RunResult {
    let context = match options {
        Some(options) => {
            execute::within(
                open_timeout,
                Box::pin(browser.new_context_with_options(options)),
            )
            .await?
        }
        None => execute::within(open_timeout, browser.new_context()).await?,
    };
    execute::with_cleanup(
        context,
        async |context| work(context).await,
        async |context| execute::within(close_timeout, context.close()).await,
    )
    .await
}

/// Opens an isolated [`BrowserContext`] and a fresh [`Page`], executes `work`, and guarantees
/// `context.close()`.
pub async fn with_page(
    browser: &Browser,
    options: Option<BrowserContextOptions>,
    open_timeout: Duration,
    close_timeout: Duration,
    work: impl AsyncFnOnce(Page) -> RunResult,
) -> RunResult {
    with_context(
        browser,
        options,
        open_timeout,
        close_timeout,
        async |context| {
            let page = execute::within(open_timeout, context.new_page()).await?;
            work(page).await
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use kat0_pipeline_reporter::error::classify_error as reporter_classify;

    use super::*;

    #[test]
    fn classify_error_maps_assertions_timeouts_and_startup() {
        assert_eq!(
            reporter_classify(
                classify_error(PlaywrightError::AssertionFailed("mismatch".into())).as_ref()
            ),
            FailureKind::Assertion
        );
        assert_eq!(
            reporter_classify(
                classify_error(PlaywrightError::AssertionTimeout("timed out".into())).as_ref()
            ),
            FailureKind::Assertion
        );
        assert_eq!(
            reporter_classify(
                classify_error(PlaywrightError::Timeout("nav timeout".into())).as_ref()
            ),
            FailureKind::Timeout
        );
        assert_eq!(
            reporter_classify(
                classify_error(PlaywrightError::LaunchFailed("no binary".into())).as_ref()
            ),
            FailureKind::Startup
        );
        assert_eq!(
            reporter_classify(
                classify_error(PlaywrightError::ProtocolError("rpc".into())).as_ref()
            ),
            FailureKind::Execution
        );
    }

    #[test]
    fn assertion_error_leaves_non_assertion_errors_unwrapped() {
        let timeout = assertion_error(PlaywrightError::Timeout("timeout".into()));
        assert!(matches!(
            timeout.downcast_ref::<PlaywrightError>(),
            Some(PlaywrightError::Timeout(_))
        ));
        let assertion = assertion_error(PlaywrightError::AssertionFailed("boom".into()));
        assert_eq!(
            reporter_classify(assertion.as_ref()),
            FailureKind::Assertion
        );
    }
}
