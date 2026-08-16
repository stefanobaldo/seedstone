use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::LazyLock;

const FIXTURES: &[&str] = &[
    "instant_now",
    "system_time_now",
    "thread_spawn",
    "rand_rng",
    "rand_random",
    "rand_random_range",
    "rand_random_bool",
    "rand_random_ratio",
    "rand_random_iter",
    "rand_fill",
    "rand_make_rng",
    "getrandom_fill",
    "getrandom_fill_uninit",
    "getrandom_u32",
    "getrandom_u64",
    "thread_rng_type",
    "sys_rng_type",
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

/// Runs the prohibition lints over one fixture and returns clippy's verdict.
///
/// `--locked` is what makes that verdict mean something over time. Each fixture
/// commits its own `Cargo.lock`, so the versions of `rand` and `getrandom` the
/// gate compiles against are the ones the prohibition paths in `clippy.toml`
/// were written for. Without the flag cargo would quietly re-resolve to
/// whatever is newest: a fixture could drift onto a release that moved or
/// renamed the item its prohibition names, and the failure would arrive as a
/// mystery on an unrelated pull request. With it, a lockfile that no longer
/// matches its manifest is a hard error naming the fixture.
fn clippy_on_fixture(name: &str) -> Output {
    let root = repo_root();
    Command::new("cargo")
        .args(["clippy", "--locked", "--quiet", "--target-dir"])
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

/// One clippy run per fixture, shared by every test that reads one.
///
/// Two tests below assert unrelated properties of the same command's output.
/// Run separately they would spend two processes per fixture to learn what one
/// already said, and — sharing a `--target-dir` — the second pass would spend
/// most of it waiting on the first's lock. Computing the runs once here keeps
/// both tests independently named and independently reported while paying for
/// the fixtures a single time.
static CLIPPY_RUNS: LazyLock<Vec<(&'static str, Output)>> = LazyLock::new(|| {
    FIXTURES
        .iter()
        .map(|name| (*name, clippy_on_fixture(name)))
        .collect()
});

#[test]
fn every_prohibition_denies_its_fixture() {
    for (name, out) in &*CLIPPY_RUNS {
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
    for (name, out) in &*CLIPPY_RUNS {
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
    let mut crates_scanned = 0;
    // Every crate, discovered from the filesystem rather than listed here. A
    // hardcoded list is a silent green waiting to happen: the crate that grows
    // the next `select!` is exactly the one nobody remembers to add.
    for src in crate_source_dirs(&root) {
        crates_scanned += 1;
        for file in rust_sources(&src) {
            let source = std::fs::read_to_string(&file).unwrap();
            // The scan below matches the qualified spelling, so an unqualified
            // `select!` would be invisible to it. Rather than teach the grep to
            // parse imports, forbid the import.
            assert!(
                !source.contains("use tokio::select"),
                "{}: importing `select!` unqualified hides it from this gate. \
                 Call it as `tokio::select!`.",
                file.display()
            );
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
    // The rule is worthless if it silently matches nothing.
    assert!(
        crates_scanned > 0,
        "no crate source directories found under crates/: this gate is looking \
         at nothing at all"
    );
    assert!(
        checked > 0,
        "no `tokio::select!` found in any crate: this gate has stopped looking \
         at the code it exists to check"
    );
}

/// The fixtures' lockfiles froze their dependency resolution — deliberately,
/// because a gate that re-resolves against the registry is a gate that flakes —
/// and that opened the one failure mode in this file which is a *silent green*:
/// the workspace moves to a new `rand` and the prohibitions go on being proven
/// against the old one, passing all the while.
///
/// The concrete instance it exists for: `rand::thread_rng` is deprecated in 0.9
/// and gone in 0.10. The day the workspace bumps, that `clippy.toml` entry names
/// an item production code can no longer call, while the fixture keeps compiling
/// 0.9.5 and keeps reporting the prohibition enforced. Nothing else here would
/// notice, because every other test in this file asks whether the fixtures are
/// refused — never whether the fixtures are still the code that ships.
///
/// So this one compares versions rather than behaviour, and turns the drift into
/// a red at the moment of the bump instead of a mystery some months later.
///
/// It failed to, once. The rand 0.10 bump (2026-08-16) sailed through the
/// original comparison, which accepted a fixture's version if the workspace
/// graph contained it *anywhere* — and `turmoil`, pinned exact, keeps the old
/// rand family alive in the graph indefinitely. Fixtures pinning 0.9 matched
/// turmoil's 0.9 and the gate stayed green while production compiled 0.10 and
/// two prohibitions went dead. Hence the rule below: a fixture must pin the
/// *newest* version the workspace resolves, because the newest is the one the
/// workspace's own crates moved to — a pinned simulator dependency only ever
/// holds the family back, never ahead.
#[test]
fn fixture_locks_agree_with_the_workspace_on_the_rand_family() {
    const WATCHED: [&str; 4] = ["rand", "rand_core", "rand_chacha", "getrandom"];

    let root = repo_root();
    let workspace = lock_versions(&root.join("Cargo.lock"), &WATCHED);
    assert!(
        !workspace.is_empty(),
        "the workspace lockfile resolves none of {WATCHED:?}: either the scanner \
         has stopped matching cargo's format or this gate is comparing nothing"
    );

    let mut compared = 0usize;
    for fixture in FIXTURES {
        // Named from `FIXTURES` rather than discovered by `read_dir`: a
        // directory walk that finds nothing passes, which is the failure mode
        // this whole test exists to remove.
        let lock = root
            .join("fixtures/determinism")
            .join(fixture)
            .join("Cargo.lock");
        for (name, version) in lock_versions(&lock, &WATCHED) {
            // The workspace may legitimately not resolve a package a fixture
            // uses at all; there is nothing to agree with then.
            let Some(newest) = workspace
                .iter()
                .filter(|(n, _)| *n == name)
                .map(|(_, v)| v)
                .max_by_key(|v| version_key(v))
            else {
                continue;
            };
            assert!(
                version == *newest,
                "{} pins {name} {version} but the workspace's newest resolution \
                 is {newest}: regenerate the fixture lockfiles so the gate proves \
                 the prohibitions against what production actually compiles",
                lock.display(),
            );
            compared += 1;
        }
    }
    assert!(
        compared > 0,
        "no fixture pinned any of {WATCHED:?}: this gate is looking at nothing"
    );
}

/// A `"major.minor.patch"` string as a sortable key.
///
/// Crude on purpose, like the scanner it serves: cargo writes bare semver
/// triples into lockfiles, and a component that fails to parse is a panic —
/// this file compares versions cargo wrote, not versions people typed.
fn version_key(v: &str) -> (u64, u64, u64) {
    let mut parts = v
        .split('.')
        .map(|p| p.parse().unwrap_or_else(|e| panic!("version {v}: {e}")));
    let mut next = || parts.next().expect("a lockfile version has three parts");
    (next(), next(), next())
}

/// `(name, version)` pairs for the watched packages of one `Cargo.lock`.
///
/// The format is a stability promise of cargo's — `[[package]]` blocks with
/// `name = "…"` then `version = "…"` — so a line scanner is enough and the test
/// adds no dependency. A lockfile that cannot be read is a panic, not a skip:
/// every path this is called with is one the repository is expected to contain.
fn lock_versions(lock: &Path, watched: &[&str]) -> Vec<(String, String)> {
    let text =
        std::fs::read_to_string(lock).unwrap_or_else(|e| panic!("reading {}: {e}", lock.display()));
    let mut out = Vec::new();
    let mut current: Option<String> = None;
    for line in text.lines() {
        if let Some(name) = line.strip_prefix("name = \"") {
            let name = name.trim_end_matches('"');
            current = watched.contains(&name).then(|| name.to_owned());
        } else if let Some(version) = line.strip_prefix("version = \"")
            && let Some(name) = current.take()
        {
            out.push((name, version.trim_end_matches('"').to_owned()));
        }
    }
    out
}

/// The `src` directory of every crate in the workspace.
fn crate_source_dirs(root: &Path) -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    for entry in
        std::fs::read_dir(root.join("crates")).expect("the workspace has a crates/ directory")
    {
        let member = entry.expect("readable crates/ entry").path();
        // The workspace is `members = ["crates/*"]`, so a member is exactly a
        // directory here carrying a manifest. Anything else under `crates/` is
        // not a crate and has no sources to scan.
        if !member.join("Cargo.toml").is_file() {
            continue;
        }
        let src = member.join("src");
        // Insisted on rather than filtered for. Skipping a member whose sources
        // are not under `src` would leave the floor below still satisfied by
        // the members that were found — and the crate that was skipped is
        // precisely the one free to hold an unbiased `select!`. One `path = "…"`
        // in a manifest is all that stands between this gate and asserting less
        // than it reports.
        assert!(
            src.is_dir(),
            "{}: a workspace member whose sources are not under src/ would be \
             scanned by nothing, in silence",
            member.display()
        );
        dirs.push(src);
    }
    // `read_dir` order is filesystem-defined; a gate that reports a different
    // file first on a different machine is a gate nobody trusts.
    dirs.sort();
    dirs
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
