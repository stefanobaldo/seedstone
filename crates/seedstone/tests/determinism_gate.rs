use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const FIXTURES: &[&str] = &["instant_now", "system_time_now", "thread_spawn", "rand_rng"];

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crate sits two levels below the repo root")
        .to_path_buf()
}

fn clippy_on_fixture(name: &str) -> Output {
    let root = repo_root();
    Command::new("cargo")
        .args(["clippy", "--quiet", "--target-dir"])
        .arg(root.join("target/fixtures"))
        .args(["--", "-D", "clippy::disallowed-methods"])
        .current_dir(root.join("fixtures/determinism").join(name))
        .env("CLIPPY_CONF_DIR", &root)
        .output()
        .expect("failed to run cargo clippy on fixture")
}

#[test]
fn every_prohibition_denies_its_fixture() {
    for name in FIXTURES {
        let out = clippy_on_fixture(name);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            !out.status.success(),
            "fixture {name} was not denied:\n{stderr}"
        );
        assert!(
            stderr.contains("disallowed"),
            "fixture {name}: the denial is not the disallowed-methods lint:\n{stderr}"
        );
        assert!(
            stderr.contains("non-deterministic"),
            "fixture {name}: our reason text is missing — a different rule fired:\n{stderr}"
        );
    }
}

#[test]
fn config_validator_is_silent() {
    // Clippy only *warns* about a malformed or unresolvable clippy.toml entry, and
    // the gate's -D warnings does not see config warnings. Failing here is what a
    // silently dead prohibition looks like.
    let out = clippy_on_fixture("instant_now");
    let stderr = String::from_utf8_lossy(&out.stderr);
    for marker in [
        "error reading Clippy's configuration file",
        "unknown field",
        // clippy 1.97 words this "does not refer to a reachable function"; older
        // and newer releases vary the tail, so anchor on the stable prefix.
        "does not refer to",
    ] {
        assert!(
            !stderr.contains(marker),
            "clippy.toml config warning ({marker}):\n{stderr}"
        );
    }
}

#[test]
fn workspace_denies_disallowed_methods() {
    let manifest = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    assert!(
        manifest.contains(r#"disallowed_methods = "deny""#),
        "the root manifest no longer denies disallowed_methods — the clippy.toml config is inert without it"
    );
}
