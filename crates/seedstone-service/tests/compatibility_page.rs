//! `docs/compatibility.md` is a claim about the command surface, and a claim
//! nothing checks is the one that goes stale. This confronts the page with
//! `command_names()`: every command the server answers has a row in the
//! *Answers* table, none has a row in *Refuses*, and nothing in *Refuses* is
//! served. A command added without a row is a red test, which is the same
//! mechanics that make `COMMAND COUNT` the table's length rather than a
//! literal.
//!
//! The parse is deliberately narrow — the first backticked token of each row
//! of two named tables — and is not a Markdown parser. Keep the page's shape
//! and this stays a twenty-line test.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

fn rows_of(page: &str, heading: &str) -> BTreeSet<String> {
    let start = page
        .find(heading)
        .unwrap_or_else(|| panic!("docs/compatibility.md has no heading {heading:?}"));
    let section = &page[start + heading.len()..];
    let end = section.find("\n## ").unwrap_or(section.len());
    section[..end]
        .lines()
        .filter(|line| line.starts_with("| `"))
        .map(|line| {
            let name = line.trim_start_matches("| `");
            let name = &name[..name.find('`').expect("a closing backtick")];
            // `CONFIG GET` and `CLIENT SETNAME` are rows about subcommands;
            // the surface is confronted at the command level.
            name.split_whitespace().next().unwrap().to_owned()
        })
        .collect()
}

#[test]
fn the_compatibility_page_matches_the_command_table() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/compatibility.md");
    let page = fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let answers = rows_of(&page, "## What it answers");
    let refuses = rows_of(&page, "## What it refuses");
    let served: BTreeSet<String> = seedstone_service::command_names()
        .map(|n| String::from_utf8(n.to_vec()).unwrap())
        .collect();

    let unlisted: Vec<_> = served.difference(&answers).collect();
    assert!(
        unlisted.is_empty(),
        "served but not in the Answers table: {unlisted:?}"
    );
    let contradicted: Vec<_> = served.intersection(&refuses).collect();
    assert!(
        contradicted.is_empty(),
        "served and listed as refused: {contradicted:?}"
    );
    let phantom: Vec<_> = answers.difference(&served).collect();
    assert!(
        phantom.is_empty(),
        "in the Answers table but not served: {phantom:?}"
    );
}
