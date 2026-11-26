// SPDX-License-Identifier: AGPL-3.0-only

use std::{
    io::{self, Write as _},
    path::Path,
    sync::Arc,
};

use crate::{
    RunResult,
    bundle::Bundle,
    http::{LocalServer, file_routes},
    wire,
};

pub(super) async fn serve(input: &Path) -> RunResult {
    let initial = wire::read(input).await?;
    let bundle = Arc::new(Bundle::embedded()?);
    let path = input.to_owned();
    let server = LocalServer::start(move |shutdown| file_routes(path, initial, bundle, shutdown))?;
    {
        let mut stdout = io::stdout().lock();
        writeln!(stdout, "{}", server.origin())?;
        stdout.flush()?;
    }
    // A caller can own this process by holding its stdin pipe open.
    let closed =
        tokio::task::spawn_blocking(|| io::copy(&mut io::stdin().lock(), &mut io::sink())).await;
    let stopped = server.close().await;
    closed??;
    stopped?;
    Ok(())
}
