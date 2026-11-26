// SPDX-License-Identifier: AGPL-3.0-only

//! Async execution combinators preserving independent failures, cleanup, and deadlines.

use std::{future::Future, io, time::Duration};

use tokio::time::timeout;

use crate::error::{BoxError, CombinedError, RunResult};

/// Runs two independent branches concurrently without cancelling either on error.
pub async fn join(
    left: impl Future<Output = RunResult>,
    right: impl Future<Output = RunResult>,
) -> RunResult {
    let (left, right) = tokio::join!(left, right);
    combine(left, right)
}

/// Runs `work` on a resource and guarantees `cleanup` runs afterwards, preserving both errors.
pub async fn with_cleanup<R>(
    mut resource: R,
    work: impl AsyncFnOnce(&mut R) -> RunResult,
    cleanup: impl AsyncFnOnce(R) -> RunResult,
) -> RunResult {
    let result = work(&mut resource).await;
    combine(result, cleanup(resource).await)
}

/// Combines two [`RunResult`] values without discarding either error.
pub fn combine(first: RunResult, second: RunResult) -> RunResult {
    match (first, second) {
        (Ok(()), other) | (other, Ok(())) => other,
        (Err(first), Err(second)) => Err(CombinedError::new(first, second).into()),
    }
}

/// Awaits `work` within `limit`, returning an `io::ErrorKind::TimedOut` error on expiration.
pub async fn within<T, E: Into<BoxError>>(
    limit: Duration,
    work: impl Future<Output = Result<T, E>>,
) -> RunResult<T> {
    timeout(limit, work)
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                format!("Operation exceeded the {}s deadline", limit.as_secs()),
            )
        })?
        .map_err(Into::into)
}
