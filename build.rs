//! Runs the paseo web app export that `src/web.rs` embeds from
//! `vendor/paseo/packages/app/dist`.
//!
//! The export is embedded where it is built. This script checks out the
//! `vendor/paseo` submodule itself when git left it missing or empty, installs
//! the submodule's dependencies with `npm ci` when they are missing, and runs
//! the app's own export command, `npm run build:web --workspace=@getpaseo/app`,
//! when `vendor/paseo/packages/app/dist` holds no export yet or when
//! `PASEO_RELAY_WEB_UI_REBUILD=true`.

use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

const REBUILD_ENV: &str = "PASEO_RELAY_WEB_UI_REBUILD";

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed={REBUILD_ENV}");

    let manifest_dir =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("cargo sets CARGO_MANIFEST_DIR"));
    let vendor = manifest_dir.join("vendor/paseo");
    let dist = vendor.join("packages/app/dist");

    ensure_submodule(&manifest_dir, &vendor);

    if flag(REBUILD_ENV) || !is_export(&dist) {
        npm_export(&vendor);
    }

    assert_export(&dist);
    // Watch the export tree instead of the files `include_dir!` listed, so that
    // adding or removing files re-embeds the app.
    println!("cargo:rerun-if-changed={}", dist.display());
}

/// Check out `vendor/paseo` when git left it missing or empty, as in a fresh
/// clone whose submodules were never initialized, so that building needs no
/// separate `git submodule update --init` step.
fn ensure_submodule(manifest_dir: &Path, vendor: &Path) {
    if is_checked_out(vendor) {
        return;
    }

    // `git submodule update` checks out the commit the parent repo records, so
    // the fetched app stays on the pinned version.
    let status = Command::new("git")
        .args(["submodule", "update", "--init", "vendor/paseo"])
        .current_dir(manifest_dir)
        .status()
        .unwrap_or_else(|e| {
            panic!(
                "failed to run `git submodule update --init vendor/paseo` to check out \
                 the vendored paseo web app: {e}\n\
                 Is git installed and on PATH?"
            )
        });
    assert!(
        status.success(),
        "`git submodule update --init vendor/paseo` in {} failed with {status} while trying \
         to check out the vendored paseo web app.\n\
         Fix the underlying cause (no network access, missing git history) or run the \
         command yourself and build again",
        manifest_dir.display()
    );
    assert!(
        is_checked_out(vendor),
        "{} was checked out but holds no package.json to build the paseo web app from",
        vendor.display()
    );
}

/// Install the app's dependencies when they are missing, then run the vendored
/// app's own export command, which writes to `packages/app/dist`.
fn npm_export(vendor: &Path) {
    // Cargo captures the script's own stdout and stderr (both need `-vv` to show
    // up) and caches `cargo:warning=` lines, replaying them on later fresh builds,
    // so neither npm command reports progress here.
    let npm = if cfg!(windows) { "npm.cmd" } else { "npm" };

    if !vendor.join("node_modules").is_dir() {
        npm_run(npm, &["ci"], vendor, "install its dependencies");
    }

    npm_run(
        npm,
        &["run", "build:web", "--workspace=@getpaseo/app"],
        vendor,
        "export the paseo web app",
    );
}

/// Run `npm` in `vendor`, failing the build with the command that was attempted.
/// Cargo prints the captured npm output along with the failure.
fn npm_run(npm: &str, args: &[&str], vendor: &Path, action: &str) {
    let command = format!("{npm} {}", args.join(" "));
    let status = Command::new(npm)
        .args(args)
        .current_dir(vendor)
        .status()
        .unwrap_or_else(|e| {
            panic!(
                "failed to run `{command}` in {} to {action}: {e}\n\
                 Is Node.js installed and on PATH?",
                vendor.display()
            )
        });
    assert!(
        status.success(),
        "`{command}` in {} failed with {status} while trying to {action}",
        vendor.display()
    );
}

fn is_export(dist: &Path) -> bool {
    dist.join("index.html").is_file()
}

/// The submodule counts as checked out once its work tree holds files: git
/// leaves an empty directory behind while the submodule stays uninitialized,
/// so testing for the directory alone cannot tell those states apart.
fn is_checked_out(vendor: &Path) -> bool {
    vendor.join("package.json").is_file()
}

fn assert_export(dist: &Path) {
    assert!(
        is_export(dist),
        "the paseo web app export at {} has no index.html; there is nothing to embed",
        dist.display()
    );
}

/// Booleans follow the runtime configuration's convention (`src/config.rs`).
fn flag(name: &str) -> bool {
    match env::var(name).as_deref() {
        Ok("true") => true,
        Ok("false") | Err(_) => false,
        Ok(other) => panic!("{name} must be true or false, got {other:?}"),
    }
}
