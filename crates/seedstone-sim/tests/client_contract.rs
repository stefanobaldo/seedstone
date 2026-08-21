//! The two halves of the simulated client's contract, held against each
//! other.
//!
//! `contract::DECLARED` says what this client can put on the wire, and a unit
//! test beside it confronts that list with the server's own command table, so
//! a command the server answers cannot go unclassified. That is the half a
//! declaration can do on its own, and it is not enough: a table can claim a
//! form the generator never reaches — a branch with probability zero, or one a
//! bug made unreachable — and nothing about the declaration would notice.
//!
//! This is the other half. A sweep runs the workload and reports which forms
//! its clients actually emitted, and the two sets have to agree in both
//! directions.

use seedstone_sim::contract::{self, Coverage};
use seedstone_sim::{SimConfig, sweep};
use std::collections::BTreeSet;

/// How many seeds the coverage sweep runs.
///
/// Enough that a form drawn on two rolls in a hundred is not missed by luck —
/// a single seed already issues hundreds of operations across its clients, so
/// this is margin rather than a requirement — and few enough to belong in a
/// test suite.
const SEEDS: u64 = 32;

/// The shape the coverage sweep runs.
///
/// The mini shape with the quiescent walk switched on, which is what makes it
/// different from the shape the gate sweeps: the complete `SCAN` cycle costs a
/// round trip per shard and is a test's to ask for, so the swept shapes leave
/// it off and never emit the forms only that cycle sends. Coverage is a
/// property of the generator rather than of the shape, and this is the shape
/// that lets the generator reach everything it can.
///
/// The gate's own sweep prints what its shape did not reach, so the difference
/// is stated there rather than hidden here.
const fn coverage_shape(sim_seed: u64) -> SimConfig {
    let mut cfg = SimConfig::mini(1, sim_seed);
    cfg.quiescent_walk = true;
    cfg
}

#[test]
fn a_sweep_reports_every_form_the_contract_claims_and_no_others() {
    let report = sweep(1, SEEDS, 4, coverage_shape, |_, _| {});
    assert_eq!(
        report.violations, 0,
        "the coverage shape has to hold before its coverage means anything"
    );

    let claimed: BTreeSet<&str> = contract::DECLARED
        .iter()
        .filter_map(|(_, coverage)| match coverage {
            Coverage::Emitted { forms } => Some(forms.iter().copied()),
            Coverage::NotEmitted { .. } => None,
        })
        .flatten()
        .collect();

    let never_seen: Vec<_> = claimed.difference(&report.forms).collect();
    assert!(
        never_seen.is_empty(),
        "the contract claims forms no seed produced: {never_seen:?}. Either the \
         workload stopped emitting them or the claim was never true."
    );
    let undeclared: Vec<_> = report.forms.difference(&claimed).collect();
    assert!(
        undeclared.is_empty(),
        "the client emitted forms the contract does not declare: {undeclared:?}"
    );
}

/// The contract's two readers must be reading the same list.
///
/// `declared_forms` is what the gate's summary subtracts what it reached from,
/// and the assertion above walks `DECLARED` itself. If the two ever disagreed
/// the gate would print a coverage line about a different denominator than the
/// one this test holds the workload to, and both would look fine.
#[test]
fn the_declared_forms_helper_reports_exactly_what_the_table_claims() {
    let from_helper: BTreeSet<&str> = contract::declared_forms().collect();
    let from_table: BTreeSet<&str> = contract::DECLARED
        .iter()
        .filter_map(|(_, coverage)| match coverage {
            Coverage::Emitted { forms } => Some(forms.iter().copied()),
            Coverage::NotEmitted { .. } => None,
        })
        .flatten()
        .collect();
    assert_eq!(from_helper, from_table);
    assert_eq!(
        from_helper.len(),
        contract::declared_forms().count(),
        "a form listed twice would make the gate's denominator larger than the \
         set of forms there are"
    );
}
