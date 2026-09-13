//! `docs/operations.md` promises the field set of every line the server
//! writes, and a promise nothing checks is the one that goes stale. This
//! confronts the page's event table with `log::EVENTS`: same events, same
//! order, same level, same fields. A field added to a line without a page
//! edit is a red test, which is the same mechanics that hold
//! `docs/compatibility.md` to `COMMAND`.
//!
//! The parse is deliberately narrow — the rows of one table under one
//! heading, three backticked cells — and is not a Markdown parser. Keep the
//! page's shape and this stays a thirty-line test.

use std::fs;
use std::path::Path;

use seedstone_service::log::EVENTS;

/// `(name, level, fields)` per row of the table under `## Output`.
fn rows(page: &str) -> Vec<(String, String, Vec<String>)> {
    let start = page
        .find("\n## Output")
        .expect("docs/operations.md has no `## Output` heading");
    let section = &page[start + "\n## Output".len()..];
    let end = section.find("\n## ").unwrap_or(section.len());
    section[..end]
        .lines()
        .filter(|line| line.starts_with("| `"))
        .map(|line| {
            let cells: Vec<&str> = line.split('|').map(str::trim).collect();
            // `["", evt, level, fields, when, ""]` — the leading and trailing
            // empties come from the outer pipes.
            assert!(cells.len() >= 5, "a row with fewer than four cells: {line}");
            let name = cells[1].trim_matches('`').to_owned();
            let level = cells[2].trim_matches('`').to_owned();
            let fields = if cells[3] == "—" {
                Vec::new()
            } else {
                cells[3]
                    .split(',')
                    .map(|f| f.trim().trim_matches('`').to_owned())
                    .collect()
            };
            (name, level, fields)
        })
        .collect()
}

#[test]
fn the_operations_page_matches_the_event_table() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/operations.md");
    let page = fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let on_page = rows(&page);
    let in_code: Vec<(String, String, Vec<String>)> = EVENTS
        .iter()
        .map(|e| {
            (
                e.name.to_owned(),
                e.level.as_str().to_owned(),
                e.fields.iter().map(|f| (*f).to_owned()).collect(),
            )
        })
        .collect();
    assert_eq!(
        on_page, in_code,
        "docs/operations.md's Output table and log::EVENTS disagree (page on the left)"
    );
}
