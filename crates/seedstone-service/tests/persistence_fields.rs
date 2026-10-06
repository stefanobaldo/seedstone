//! `docs/operations.md`'s `INFO persistence` table names exactly the fields
//! the section can print, in print order.

use seedstone_service::PERSISTENCE_FIELDS;
use std::{fs, path::Path};

/// The first cell of every row of the section's table.
fn rows(page: &str) -> Vec<String> {
    let heading = "\n## What `INFO persistence` reports";
    let start = page
        .find(heading)
        .expect("docs/operations.md has no INFO persistence heading");
    let section = &page[start + heading.len()..];
    let end = section.find("\n## ").unwrap_or(section.len());
    section[..end]
        .lines()
        .filter(|line| line.starts_with("| `"))
        .map(|line| {
            let name = line.trim_start_matches("| `");
            name[..name.find('`').unwrap()].to_owned()
        })
        .collect()
}

#[test]
fn the_operations_page_names_every_persistence_field_in_order() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/operations.md");
    let page = fs::read_to_string(&path).unwrap();
    assert_eq!(rows(&page), PERSISTENCE_FIELDS);
}
