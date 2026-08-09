use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const FIXTURES: &[&str] = &[
    "instant_now",
    "system_time_now",
    "thread_spawn",
    "rand_rng",
    "rand_thread_rng",
    "rand_random",
    "rand_random_range",
    "rand_random_bool",
    "rand_random_ratio",
    "rand_random_iter",
    "rand_fill",
    "seedable_from_os_rng",
    "seedable_try_from_os_rng",
    "getrandom_fill",
    "getrandom_fill_uninit",
    "getrandom_u32",
    "getrandom_u64",
    "thread_rng_type",
    "os_rng_type",
    "hashmap_default",
    "hashset_default",
];

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
        .args([
            "--",
            "-D",
            "clippy::disallowed-methods",
            "-D",
            "clippy::disallowed-types",
        ])
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
    //
    // Clippy can only tell that a path is unresolvable when the crate it names is
    // loaded into the compilation, so this has to run over every fixture: each one
    // pulls in the crate its own prohibition names.
    for name in FIXTURES {
        let out = clippy_on_fixture(name);
        let stderr = String::from_utf8_lossy(&out.stderr);
        for marker in [
            "error reading Clippy's configuration file",
            "unknown field",
            // clippy 1.97 words this "does not refer to a reachable function";
            // older and newer releases vary the tail, so anchor on the stable
            // prefix.
            "does not refer to",
        ] {
            assert!(
                !stderr.contains(marker),
                "fixture {name}: clippy.toml config warning ({marker}):\n{stderr}"
            );
        }
    }
}

#[test]
fn workspace_denies_the_prohibition_lints() {
    let manifest = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    for lint in ["disallowed_methods", "disallowed_types"] {
        assert!(
            manifest.contains(&format!(r#"{lint} = "deny""#)),
            "the root manifest no longer denies {lint} — the clippy.toml config is inert without it"
        );
    }
}

/// Every `tokio::select!` reachable from the simulator must be `biased`.
///
/// `select!` picks among ready arms at random, from a generator seeded by the
/// OS. turmoil seeds tokio's runtime RNG and so removes that entropy — but
/// only under `--cfg tokio_unstable`, which this workspace deliberately does
/// not set: it is a workspace-wide rustflag, so it cannot be scoped to the
/// simulator, and it would compile tokio's unstable surface into the
/// production binary the project intends to benchmark. That decision is
/// revisited once a baseline benchmark exists to price it.
///
/// The consequence is that `biased` is not a style preference here. It is the
/// only thing standing between a seed and a run that does not reproduce, and
/// no clippy lint can enforce it: `disallowed_methods` matches paths, and this
/// lives inside a macro body.
///
/// The check is deliberately crude — the source text, not the token stream —
/// in the same spirit as the manifest assertion above. A crude check that runs
/// beats an exact one that does not exist.
#[test]
fn every_select_in_simulated_code_is_biased() {
    let root = repo_root();
    let mut checked = 0;
    for crate_name in ["seedstone-core", "seedstone-sim", "seedstone-resp"] {
        for file in rust_sources(&root.join("crates").join(crate_name).join("src")) {
            let source = std::fs::read_to_string(&file).unwrap();
            for (offset, _) in source.match_indices("tokio::select!") {
                // The line the macro opens on, plus what follows it. `biased;`
                // must be the first statement inside the braces.
                let body = &source[offset..];
                let after_brace = body
                    .find('{')
                    .map(|brace| &body[brace + 1..])
                    .unwrap_or_default();
                assert!(
                    after_brace
                        .lines()
                        .map(str::trim)
                        .find(|line| !line.is_empty() && !line.starts_with("//"))
                        == Some("biased;"),
                    "{}: a `tokio::select!` here is not `biased`.\n\
                     Arm choice would then come from the runtime's RNG, which is \
                     seeded from OS entropy — turmoil only seeds it under \
                     `--cfg tokio_unstable`, which this workspace does not set \
                     (see this test's documentation for why). A seed would stop \
                     reproducing its run, and no lint can catch it.",
                    file.display()
                );
                checked += 1;
            }
        }
    }
    // The rule is worthless if it silently matches nothing — a renamed crate
    // directory, or `select!` imported unqualified, would make this test pass
    // over an empty set forever.
    assert!(
        checked > 0,
        "no `tokio::select!` found in any simulated crate: this gate has stopped \
         looking at the code it exists to check"
    );
}

/// Every `.rs` file under `dir`, recursively.
fn rust_sources(dir: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(path) = stack.pop() {
        for entry in std::fs::read_dir(&path).expect("a crate's src directory must exist") {
            let entry = entry.unwrap().path();
            if entry.is_dir() {
                stack.push(entry);
            } else if entry.extension().is_some_and(|ext| ext == "rs") {
                found.push(entry);
            }
        }
    }
    found
}
