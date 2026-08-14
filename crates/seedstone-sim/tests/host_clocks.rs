//! The property the two expiration bands rest on: simulated hosts' clocks do
//! not drift apart.
//!
//! `STALE_SLACK` pays for one message's travel and `LIVE_SLACK` pays nothing,
//! and both derivations take the same step — that a duration measured on one
//! host is the same duration measured on another. A client records its deadline
//! and its reads from its own clock; the server sets and honours the real
//! deadline on its own. Neither ever reads the other's, so the offset between
//! them — each host's paused clock starts at whatever the wall clock said when
//! turmoil built its runtime, which is hundreds of microseconds of spread on a
//! typical machine — cancels out of every difference either side takes.
//!
//! Drift would not cancel. If one host's clock advanced by more simulated time
//! than another's over the same stretch of a run, a band derived on the
//! assumption that it did not would be too narrow by the difference, and the
//! harness would report expiration violations the server never committed.
//!
//! This is turmoil's behaviour rather than ours, which is exactly why it is
//! asserted here: the bands are two constants in `lib.rs` that no upgrade of a
//! dependency would think to revisit.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// When each host takes its first reading — early, but after every host has
/// certainly started.
const FIRST: Duration = Duration::from_millis(100);

/// When each host takes its second. Past the longest deadline the workload
/// hands out and past the settle that waits for it, so the span covers more of
/// a run than a run does.
const SECOND: Duration = Duration::from_millis(1500);

/// What every host's clock must advance by between the two readings.
const SPAN: Duration = SECOND.saturating_sub(FIRST);

/// A host's name and its two clock readings.
type Stamps = Arc<Mutex<Vec<(String, Instant, Instant)>>>;

/// Reads this host's clock at [`FIRST`] and again at [`SECOND`].
async fn stamp(name: String, stamps: Stamps) {
    tokio::time::sleep(FIRST).await;
    let first = tokio::time::Instant::now().into_std();
    tokio::time::sleep(SPAN).await;
    let second = tokio::time::Instant::now().into_std();
    stamps.lock().expect("stamps").push((name, first, second));
}

/// Runs one host set and returns what each host's clock did.
fn run(clients: u16, sim_seed: u64) -> Vec<(String, Instant, Instant)> {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_mins(1))
        .rng_seed(sim_seed)
        .build();

    let stamps: Stamps = Arc::new(Mutex::new(Vec::new()));

    // The server is a host rather than a client: it never finishes, exactly as
    // the real one does not, so the run ends when the clients do.
    let server_stamps = Arc::clone(&stamps);
    sim.host("server", move || {
        let stamps = Arc::clone(&server_stamps);
        async move {
            stamp("server".to_owned(), stamps).await;
            std::future::pending::<()>().await;
            Ok(())
        }
    });
    for id in 0..clients {
        let stamps = Arc::clone(&stamps);
        sim.client(format!("client-{id}"), async move {
            stamp(format!("client-{id}"), stamps).await;
            Ok(())
        });
    }
    let verifier_stamps = Arc::clone(&stamps);
    sim.client("verifier", async move {
        stamp("verifier".to_owned(), verifier_stamps).await;
        Ok(())
    });

    sim.run().expect("simulation failed");
    let stamps = stamps.lock().expect("stamps").clone();
    assert_eq!(
        stamps.len(),
        usize::from(clients) + 2,
        "a host never reported: the measurement below would be of a subset"
    );
    stamps
}

/// Every host advances by the same simulated time over the same stretch of a
/// run.
///
/// Two seeds, because the seed decides the order turmoil steps its hosts in,
/// which is the one thing here that varies between runs. The host *count* is
/// not swept: a simulation costs wall clock in proportion to hosts times ticks,
/// and the standard shape's 128 clients multiply this by seven for no reach —
/// the tick is global, so a count that changed this would change it at two
/// hosts as well. The count used is the mini shape's, and both kinds of host
/// are present, since turmoil starts a `host` and a `client` differently.
#[test]
fn no_simulated_host_gains_on_another() {
    for sim_seed in [1u64, 2] {
        let stamps = run(16, sim_seed);
        for (name, first, second) in &stamps {
            assert_eq!(
                second.duration_since(*first),
                SPAN,
                "host {name} advanced by something other than the time it \
                 slept, on seed {sim_seed}: the expiration bands assume a \
                 duration means the same thing on every host"
            );
        }
    }
}

/// The offset between hosts is real, and it is the thing the bands do *not*
/// pay for — so this test says out loud that it exists rather than leaving a
/// future reader to wonder whether it was overlooked.
///
/// It is not asserted against a bound: it is however long the machine took to
/// build the runtimes, which is not a property of the simulation. What matters
/// is that it is the same at both readings, which is
/// [`no_simulated_host_gains_on_another`] restated as the quantity the
/// derivation actually names.
#[test]
fn the_offset_between_hosts_is_constant_however_large_it_is() {
    let stamps = run(16, 1);

    let spread = |at: fn(&(String, Instant, Instant)) -> Instant| {
        let min = stamps.iter().map(at).min().expect("a host");
        let max = stamps.iter().map(at).max().expect("a host");
        max.duration_since(min)
    };

    assert_eq!(
        spread(|(_, first, _)| *first),
        spread(|(_, _, second)| *second),
        "the hosts' clocks were further apart at one reading than the other"
    );
}
