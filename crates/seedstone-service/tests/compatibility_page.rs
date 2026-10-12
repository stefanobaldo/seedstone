//! `docs/compatibility.md` is a claim about the command surface, and a claim
//! nothing checks is the one that goes stale. This confronts the page with
//! `command_names()`: every command the server answers has a row in the
//! *Answers* table, none has a row in *Refuses*, and nothing in *Refuses* is
//! served. A command added without a row is a red test, which is the same
//! mechanics that make `COMMAND COUNT` the table's length rather than a
//! literal. The README's command table is held to the same list, both ways.
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

/// The README's "What it speaks" table names every command the server
/// answers and nothing else, at the command level (`CONFIG`, not
/// `CONFIG GET`). The parse is the backticked tokens of the table rows
/// under that heading, nothing more.
fn readme_commands(page: &str) -> BTreeSet<String> {
    let start = page
        .find("## What it speaks")
        .expect("README.md has no \"What it speaks\" heading");
    let section = &page[start..];
    let end = section[3..].find("\n## ").map_or(section.len(), |i| i + 3);
    section[..end]
        .lines()
        .filter(|line| line.starts_with("| ") && line.contains('`'))
        .flat_map(|line| {
            line.split('`')
                .skip(1)
                .step_by(2)
                .map(|tok| tok.split_whitespace().next().unwrap_or("").to_owned())
                .collect::<Vec<_>>()
        })
        .filter(|tok| !tok.is_empty())
        .collect()
}

#[test]
fn the_readme_table_matches_the_command_table() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../README.md");
    let page = fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let listed = readme_commands(&page);
    let served: BTreeSet<String> = seedstone_service::command_names()
        .map(|n| String::from_utf8(n.to_vec()).unwrap())
        .collect();
    let unlisted: Vec<_> = served.difference(&listed).collect();
    assert!(
        unlisted.is_empty(),
        "served but not on the README's table: {unlisted:?}"
    );
    let phantom: Vec<_> = listed.difference(&served).collect();
    assert!(
        phantom.is_empty(),
        "on the README's table but not served: {phantom:?}"
    );
}
