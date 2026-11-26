// SPDX-License-Identifier: AGPL-3.0-only

use std::{
    error::Error,
    fs,
    io::Write as _,
    path::{Path, PathBuf, absolute},
    process::{Command, Stdio},
};

use serde::{Deserialize, Serialize};

use crate::sha256;

const BUILD_CMD: &str = "cargo leptos build --release --frontend-only --cargo-locked";
const OUTPUT_NAME: &str = "kat0-pipeline";
const PKG_DIR: &str = "pkg";
const PANEL_JS: &str = "panel.js";
const PANEL_WASM: &str = "panel.wasm";
const PANEL_CSS: &str = "report.css";
const MANIFEST_FILE: &str = "manifest.json";
/// Keep embedded panel assets well below crates.io's 10 MiB compressed package limit.
const MAX_TOTAL_ASSET_BYTES: usize = 5 * 1024 * 1024;

type TaskResult<T = ()> = Result<T, Box<dyn Error>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AssetEntry {
    file: String,
    bytes: usize,
    sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AssetManifest {
    schema_version: u32,
    total_bytes: usize,
    assets: Vec<AssetEntry>,
}

struct BuiltAssets {
    js: Vec<u8>,
    wasm: Vec<u8>,
    css: Vec<u8>,
}

impl BuiltAssets {
    fn entries(&self) -> [(&'static str, &[u8]); 3] {
        [
            (PANEL_JS, &self.js),
            (PANEL_WASM, &self.wasm),
            (PANEL_CSS, &self.css),
        ]
    }

    fn total_bytes(&self) -> usize {
        self.js.len() + self.wasm.len() + self.css.len()
    }

    fn manifest(&self) -> AssetManifest {
        let assets = self
            .entries()
            .into_iter()
            .map(|(file, bytes)| AssetEntry {
                file: file.to_owned(),
                bytes: bytes.len(),
                sha256: sha256::digest_hex(bytes),
            })
            .collect();
        AssetManifest {
            schema_version: 1,
            total_bytes: self.total_bytes(),
            assets,
        }
    }
}

pub fn build_and_stage() -> TaskResult {
    let root = workspace_root();
    let built = build_frontend(&root)?;
    check_size_budget(built.total_bytes())?;

    let assets_dir = server_assets_dir(&root);
    fs::create_dir_all(&assets_dir)?;
    for (file, bytes) in built.entries() {
        write_atomic(&assets_dir.join(file), bytes)?;
    }

    let manifest = built.manifest();
    let mut manifest_json = serde_json::to_vec_pretty(&manifest)?;
    manifest_json.push(b'\n');
    write_atomic(&assets_dir.join(MANIFEST_FILE), &manifest_json)?;

    println!(
        "Staged panel assets in {} ({} bytes total):",
        assets_dir.display(),
        manifest.total_bytes
    );
    for entry in &manifest.assets {
        println!(
            "  {} — {} bytes (sha256:{})",
            entry.file,
            entry.bytes,
            &entry.sha256[..12]
        );
    }
    Ok(())
}

pub fn verify(rebuild: bool) -> TaskResult {
    let root = workspace_root();
    let assets_dir = server_assets_dir(&root);
    let manifest_bytes = fs::read(assets_dir.join(MANIFEST_FILE)).map_err(|error| {
        format!(
            "Falta {}: {error}. Ejecuta `cargo run -p xtask -- build-panel`.",
            assets_dir.join(MANIFEST_FILE).display()
        )
    })?;
    let manifest: AssetManifest = serde_json::from_slice(&manifest_bytes)?;
    verify_manifest_on_disk(&assets_dir, &manifest)?;

    if rebuild {
        let rebuilt = build_frontend(&root)?;
        let expected = rebuilt.manifest();
        if manifest != expected {
            return Err(
                "Los assets en server/assets/ no coinciden con la compilación actual de client/. \
                 Ejecuta `cargo run -p xtask -- build-panel`."
                    .into(),
            );
        }
        println!(
            "Verified reproducible panel assets ({} bytes total).",
            manifest.total_bytes
        );
    } else {
        println!(
            "Verified panel asset manifest ({} bytes total).",
            manifest.total_bytes
        );
    }
    Ok(())
}

fn verify_manifest_on_disk(assets_dir: &Path, manifest: &AssetManifest) -> TaskResult {
    if manifest.schema_version != 1 {
        return Err(format!(
            "Versión de manifest no admitida: {}",
            manifest.schema_version
        )
        .into());
    }
    let expected_files = [PANEL_JS, PANEL_WASM, PANEL_CSS];
    if manifest.assets.len() != expected_files.len() {
        return Err(
            "El manifest debe contener exactamente panel.js, panel.wasm y report.css".into(),
        );
    }
    let mut total = 0usize;
    for (entry, expected_name) in manifest.assets.iter().zip(expected_files) {
        if entry.file != expected_name {
            return Err(format!(
                "Se esperaba {expected_name:?} en el manifest, pero apareció {:?}",
                entry.file
            )
            .into());
        }
        let path = assets_dir.join(&entry.file);
        let bytes = fs::read(&path)
            .map_err(|error| format!("No se pudo leer {}: {error}", path.display()))?;
        if bytes.len() != entry.bytes {
            return Err(format!(
                "Tamaño distinto para {}: esperado {}, actual {}",
                entry.file,
                entry.bytes,
                bytes.len()
            )
            .into());
        }
        let actual_hash = sha256::digest_hex(&bytes);
        if actual_hash != entry.sha256 {
            return Err(format!(
                "SHA-256 distinto para {}: esperado {}, actual {actual_hash}",
                entry.file, entry.sha256
            )
            .into());
        }
        total += bytes.len();
    }
    if total != manifest.total_bytes {
        return Err(format!(
            "total_bytes del manifest ({}) no coincide con la suma real ({total})",
            manifest.total_bytes
        )
        .into());
    }
    check_size_budget(total)?;
    Ok(())
}

fn check_size_budget(total_bytes: usize) -> TaskResult {
    if total_bytes == 0 {
        return Err("Los assets del panel no pueden estar vacíos".into());
    }
    if total_bytes > MAX_TOTAL_ASSET_BYTES {
        return Err(format!(
            "Los assets del panel ({total_bytes} bytes) superan el presupuesto máximo de {MAX_TOTAL_ASSET_BYTES} bytes"
        )
        .into());
    }
    Ok(())
}

fn build_frontend(root: &Path) -> TaskResult<BuiltAssets> {
    let target = absolute(root.join("target"))?;
    fs::create_dir_all(&target)?;
    // cargo-leptos empties the site root at the start of each frontend build.
    let site = tempfile::Builder::new()
        .prefix("pipeline-site-")
        .tempdir_in(&target)?;
    let site_root = site
        .path()
        .to_str()
        .ok_or("El directorio temporal para cargo-leptos debe usar UTF-8")?;

    let output = Command::new("cargo")
        .current_dir(root)
        .args([
            "leptos",
            "build",
            "--release",
            "--frontend-only",
            "--cargo-locked",
        ])
        .env("LEPTOS_SITE_ROOT", site_root)
        .env("LEPTOS_SITE_PKG_DIR", PKG_DIR)
        .env("LEPTOS_OUTPUT_NAME", OUTPUT_NAME)
        .env("LEPTOS_HASH_FILES", "false")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .map_err(|error| format!("No se pudo ejecutar {BUILD_CMD}: {error}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !stderr.trim().is_empty() {
            eprintln!("{stderr}");
        }
        return Err(format!("{BUILD_CMD} falló ({})", output.status).into());
    }

    let [js, wasm, css] = read_pkg(&site.path().join(PKG_DIR))?;
    Ok(BuiltAssets { js, wasm, css })
}

fn read_pkg(pkg: &Path) -> TaskResult<[Vec<u8>; 3]> {
    let entries =
        fs::read_dir(pkg).map_err(|error| format!("No se pudo leer pkg del informe: {error}"))?;
    let (mut js, mut wasm, mut css) = (None, None, None);
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_str().ok_or("Nombre de artefacto no UTF-8 en pkg")?;
        if !entry.file_type()?.is_file() || name.starts_with("__wasm_split") {
            return Err(
                "El informe autónomo no admite snippets, WASM split ni otros recursos externos"
                    .into(),
            );
        }
        let slot = match name.rsplit_once('.').map(|(_, ext)| ext) {
            Some("js") => &mut js,
            Some("wasm") => &mut wasm,
            Some("css") => &mut css,
            Some("ts") if name.ends_with(".d.ts") => continue,
            _ => return Err(format!("Artefacto inesperado en pkg: {name:?}").into()),
        };
        if slot.replace(entry.path()).is_some() {
            return Err(
                format!("Se requiere un solo JS, WASM y CSS en pkg; sobra {name:?}").into(),
            );
        }
    }
    Ok([
        read_asset(js, "js")?,
        read_asset(wasm, "wasm")?,
        read_asset(css, "css")?,
    ])
}

fn read_asset(path: Option<PathBuf>, ext: &str) -> TaskResult<Vec<u8>> {
    let path =
        path.ok_or_else(|| format!("cargo-leptos no generó el archivo .{ext} del informe"))?;
    fs::read(path).map_err(|error| format!("No se pudo leer el .{ext} del informe: {error}").into())
}

fn write_atomic(path: &Path, bytes: &[u8]) -> TaskResult {
    let parent = path
        .parent()
        .ok_or("La ruta de destino debe tener directorio padre")?;
    let mut staged = tempfile::Builder::new()
        .prefix(".asset-")
        .tempfile_in(parent)?;
    staged.write_all(bytes)?;
    staged.as_file().sync_all()?;
    staged.persist(path).map_err(|error| error.error)?;
    Ok(())
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask lives under the pipeline workspace root")
        .to_owned()
}

fn server_assets_dir(workspace_root: &Path) -> PathBuf {
    workspace_root.join("server/assets")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_unique_assets_without_requiring_typescript_declarations() {
        let dir = tempfile::tempdir().unwrap();
        let pkg = dir.path();
        fs::write(pkg.join("panel.js"), b"js").unwrap();
        fs::write(pkg.join("panel.css"), b"css").unwrap();
        assert!(read_pkg(pkg).is_err());
        fs::write(pkg.join("panel.wasm"), b"\0asm").unwrap();
        let expected = [b"js".to_vec(), b"\0asm".to_vec(), b"css".to_vec()];
        assert_eq!(read_pkg(pkg).unwrap(), expected);
        fs::write(pkg.join("panel.d.ts"), b"").unwrap();
        assert_eq!(read_pkg(pkg).unwrap(), expected);

        for name in ["extra.js", "__wasm_split_manifest.json"] {
            let extra = pkg.join(name);
            fs::write(&extra, b"").unwrap();
            assert!(read_pkg(pkg).is_err(), "{name}");
            fs::remove_file(extra).unwrap();
        }
        fs::create_dir(pkg.join("snippets")).unwrap();
        assert!(read_pkg(pkg).is_err());
    }

    #[test]
    fn manifest_verification_detects_corruption_and_size_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let built = BuiltAssets {
            js: b"export function hydrate() {}".to_vec(),
            wasm: b"\0asm\x01\0\0\0".to_vec(),
            css: b"body{margin:0}".to_vec(),
        };
        for (file, bytes) in built.entries() {
            fs::write(dir.path().join(file), bytes).unwrap();
        }
        let manifest = built.manifest();
        verify_manifest_on_disk(dir.path(), &manifest).unwrap();

        fs::write(dir.path().join(PANEL_CSS), b"body{margin:1}").unwrap();
        assert!(verify_manifest_on_disk(dir.path(), &manifest).is_err());
    }
}
