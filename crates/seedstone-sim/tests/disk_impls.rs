//! The filesystem seam has exactly the implementations it is supposed to:
//! the real one, the simulator's, and the in-memory one the core's tests
//! use. A fourth would be a second production disk or a second simulated
//! one, and either is a question to ask before it is a file to review.

use std::fs;
use std::path::Path;

#[test]
fn the_disk_seam_has_exactly_three_implementations() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let mut found = Vec::new();
    for crate_dir in [
        "seedstone-core",
        "seedstone-service",
        "seedstone-resp",
        "seedstone-sim",
        "seedstone",
    ] {
        walk(&root.join(crate_dir).join("src"), &mut found);
        let tests = root.join(crate_dir).join("tests");
        if tests.exists() {
            walk(&tests, &mut found);
        }
    }
    found.sort();
    assert_eq!(
        found,
        ["MemDisk", "SimDisk", "StdDisk"],
        "the seam's implementations changed"
    );
}

fn walk(dir: &Path, found: &mut Vec<String>) {
    for entry in fs::read_dir(dir).expect("a crate directory") {
        let path = entry.expect("an entry").path();
        if path.is_dir() {
            walk(&path, found);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            let text = fs::read_to_string(&path).expect("a source file");
            for line in text.lines() {
                if let Some(rest) = line.trim().strip_prefix("impl Disk for ") {
                    found.push(rest.trim_end_matches(" {").to_owned());
                }
            }
        }
    }
}
