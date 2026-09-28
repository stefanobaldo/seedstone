//! The simulator's filesystem behind the seam: a segment written and
//! synced on one host survives that host's crash, and one left pending
//! does not.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use seedstone_core::log::disk::{Disk, LogFile};
use seedstone_sim::SimDisk;

#[test]
fn a_synced_write_survives_a_crash_and_a_pending_one_does_not() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(10))
        .rng_seed(1)
        .build();
    let seen: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let starts = Arc::new(Mutex::new(0u32));
    sim.host("server", {
        let seen = Arc::clone(&seen);
        let starts = Arc::clone(&starts);
        move || {
            let seen = Arc::clone(&seen);
            let starts = Arc::clone(&starts);
            async move {
                let disk = SimDisk;
                let dir = Path::new("/data");
                disk.create_dir_all(dir)?;
                let path = dir.join("f");
                let start = {
                    let mut starts = starts.lock().unwrap();
                    *starts += 1;
                    *starts
                };
                if start == 1 {
                    let mut file = disk.create_append(&path)?;
                    file.write_all(b"synced")?;
                    file.sync_data()?;
                    disk.sync_dir(dir)?;
                    file.write_all(b"+pending")?;
                    // Park until the driver crashes us.
                    tokio::time::sleep(Duration::from_secs(100)).await;
                } else {
                    let mut bytes = Vec::new();
                    std::io::Read::read_to_end(&mut disk.open_read(&path)?, &mut bytes)?;
                    seen.lock().unwrap().push(bytes);
                }
                Ok(())
            }
        }
    });
    sim.client("driver", async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    });
    sim.run().unwrap();
    sim.crash("server");
    sim.bounce("server");
    sim.client("driver2", async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    });
    sim.run().unwrap();
    assert_eq!(seen.lock().unwrap().as_slice(), [b"synced".to_vec()]);
}

#[test]
fn the_world_clock_is_the_same_on_every_host_and_moves() {
    use seedstone_sim::world_now;
    let readings: Arc<Mutex<Vec<(&'static str, Duration)>>> = Arc::new(Mutex::new(Vec::new()));
    let mut sim = turmoil::Builder::new().rng_seed(1).build();
    for name in ["a", "b"] {
        let readings = Arc::clone(&readings);
        sim.client(name, async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            readings.lock().unwrap().push((name, world_now()));
            tokio::time::sleep(Duration::from_millis(100)).await;
            readings.lock().unwrap().push((name, world_now()));
            Ok(())
        });
    }
    sim.run().unwrap();
    let readings = readings.lock().unwrap();
    let at = |name: &str, nth: usize| {
        readings
            .iter()
            .filter(|(n, _)| *n == name)
            .nth(nth)
            .unwrap()
            .1
    };
    assert_eq!(at("a", 0), at("b", 0), "one world clock");
    assert!(at("a", 1) > at("a", 0), "and it moves");
}
