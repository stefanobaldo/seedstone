//! What a start says about the shards it cut, before it fails or serves,
//! and what it writes when it cut none.

use super::*;

/// A start that cut shards and then cannot open its log says what it cut
/// before it fails: `recovery`, the `recovery_truncated` lines, then
/// `recovery_failed`, and exit code 1. Once the damage is gone and the
/// directory writable, the next start serves every write — nothing the
/// failed start read was removed.
///
/// Two processes write to one shard, so the log has two segments, the
/// newer one wholly past a byte flipped inside the older one. The
/// directory is made read-only for the second start: recovery still
/// reads it, and the writer cannot create its segment. A process that
/// ignores the permission (root) cannot be made to fail this way, and the
/// test says so and stops.
#[tokio::test(flavor = "multi_thread")]
async fn a_cut_start_that_cannot_open_its_log_reports_the_cut_then_fails() {
    use std::os::unix::fs::PermissionsExt;
    let dir = scratch();
    let stderr = dir.with_extension("stderr");
    let keys = keys_on_one_executor(8);
    let mut expected = std::collections::BTreeMap::new();
    for run in 0..2 {
        let (node, port) = start_with(&dir, &stderr, &["--fsync", "always"]).await;
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        for i in 0..100 {
            let key = &keys[i % keys.len()];
            let value = format!("{run}-{i}");
            assert_eq!(
                round_trip(&mut stream, &["SET", key, &value]).await,
                Frame::Simple("OK".into())
            );
            expected.insert(key.clone(), value);
        }
        terminate_and_wait(node);
    }
    let wal = dir.join("wal");
    let segments = segment_names(&dir);
    assert_eq!(segments.len(), 2, "{segments:?}");
    let older = wal.join(&segments[0]);
    let good = std::fs::read(&older).unwrap();
    let mut bad = good.clone();
    let middle = data_record_at(&good, good.len() / 2);
    bad[middle] ^= 0xFF;
    std::fs::write(&older, &bad).unwrap();

    std::fs::set_permissions(&wal, std::fs::Permissions::from_mode(0o555)).unwrap();
    if std::fs::write(wal.join("probe"), b"").is_ok() {
        eprintln!("this process writes to a read-only directory; nothing to provoke");
        std::fs::set_permissions(&wal, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
        return;
    }
    let failed_err = dir.with_extension("stderr-failed");
    let mut failed = spawn(
        &dir,
        &[
            "--data-dir".as_ref(),
            dir.as_os_str(),
            "--fsync".as_ref(),
            "always".as_ref(),
        ],
        &failed_err,
    );
    let status = failed.wait().expect("reaped");
    std::fs::set_permissions(&wal, std::fs::Permissions::from_mode(0o755)).unwrap();
    let text = std::fs::read_to_string(&failed_err).unwrap();
    assert_eq!(status.code(), Some(1), "{text}");
    let at = |evt: &str| {
        text.lines()
            .position(|line| line.contains(&format!("\"evt\":\"{evt}\"")))
            .unwrap_or_else(|| panic!("no {evt} line: {text}"))
    };
    let (recovery, truncated, failed_at) = (
        at("recovery"),
        at("recovery_truncated"),
        at("recovery_failed"),
    );
    assert!(recovery < truncated && truncated < failed_at, "{text}");

    std::fs::write(&older, &good).unwrap();
    let (node, port) = start_with(&dir, &stderr, &["--fsync", "always"]).await;
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    for (key, value) in &expected {
        assert_eq!(
            round_trip(&mut stream, &["GET", key]).await,
            Frame::Bulk(value.as_bytes().to_vec().into()),
            "{key}"
        );
    }
    terminate_and_wait(node);
    std::fs::remove_dir_all(&dir).unwrap();
    std::fs::remove_file(&stderr).unwrap();
    std::fs::remove_file(&failed_err).unwrap();
}

/// The middle of the first key's record in `segment` that ends past
/// `from`: a byte whose damage costs a shard a record, not one inside a
/// sync mark, which belongs to no shard.
fn data_record_at(segment: &[u8], from: usize) -> usize {
    use seedstone_core::log::file::{SYNC_MARK_SHARD, decode_segment_header};
    use seedstone_core::log::{Decoded, decode_record};
    let mut at = decode_segment_header(segment).unwrap().len;
    loop {
        let Decoded::Record {
            shard, consumed, ..
        } = decode_record(&segment[at..])
        else {
            panic!("no key's record past byte {from}")
        };
        if shard != SYNC_MARK_SHARD && at + consumed > from {
            return at + consumed / 2;
        }
        at += consumed;
    }
}

/// A start with nothing cut writes nothing but its segment's header and
/// reports as it always has: a `recovery` line with nothing truncated or
/// lossy, no `recovery_truncated`, no `recovery_failed`, every key served.
#[tokio::test(flavor = "multi_thread")]
async fn a_start_with_no_cut_writes_no_rebase_and_reports_as_before() {
    let dir = scratch();
    let stderr = dir.with_extension("stderr");
    let segment_sizes = || -> Vec<(String, u64)> {
        segment_names(&dir)
            .into_iter()
            .map(|name| {
                let len = std::fs::metadata(dir.join("wal").join(&name))
                    .unwrap()
                    .len();
                (name, len)
            })
            .collect()
    };
    let (first, port) = start_with(&dir, &stderr, &["--fsync", "always"]).await;
    let header = segment_sizes()[0].1;
    set_many(port, 50).await;
    terminate_and_wait(first);
    let (second, port) = start_with(&dir, &stderr, &["--fsync", "always"]).await;
    let sizes = segment_sizes();
    assert_eq!(sizes.len(), 2, "{sizes:?}");
    assert_eq!(sizes[1].1, header, "the new segment holds its header alone");
    assert_eq!(dbsize(port).await, Frame::Integer(50));
    let text = std::fs::read_to_string(&stderr).unwrap();
    let recovery = text
        .lines()
        .find(|line| line.contains("\"evt\":\"recovery\""))
        .unwrap_or_else(|| panic!("no recovery line: {text}"));
    for name in ["discarded", "truncated_shards", "lossy_shards", "holes"] {
        assert_eq!(field(recovery, name), 0, "{name}: {recovery}");
    }
    assert_eq!(field(recovery, "applied"), 50, "{recovery}");
    assert!(!text.contains("\"evt\":\"recovery_truncated\""), "{text}");
    assert!(!text.contains("\"evt\":\"recovery_failed\""), "{text}");
    terminate_and_wait(second);
    std::fs::remove_dir_all(&dir).unwrap();
    std::fs::remove_file(&stderr).unwrap();
}
