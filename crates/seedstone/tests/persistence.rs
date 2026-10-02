//! The binary, killed and restarted on the same directory, serves what it
//! acknowledged before the last sync.
//!
//! The one test in the tree that runs the release path end to end: a real
//! process, a real `SIGKILL`, a real directory. Everything below this is
//! exercised on an in-memory disk or the simulator's; this is what says the
//! `std::fs` half of the seam was wired into the composition root.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use seedstone_resp::{Frame, encode, parse};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

static DIRS: AtomicU64 = AtomicU64::new(0);

/// A directory of this test's own, named so two tests in one process
/// cannot share one, created empty.
fn scratch() -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "seedstone-persistence-{}-{}",
        std::process::id(),
        DIRS.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Starts the binary on an ephemeral port, in `cwd`, with `args` after the
/// bind, its stderr written to `stderr`.
///
/// Stderr goes to a file rather than a pipe: a pipe must be drained for as
/// long as the child lives, and a file is also there to read when an
/// assertion fails.
fn spawn(cwd: &Path, args: &[&std::ffi::OsStr], stderr: &Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_seedstone"))
        .args(["--bind", "127.0.0.1:0"])
        .args(args)
        .current_dir(cwd)
        .stderr(std::fs::File::create(stderr).unwrap())
        .stdout(Stdio::null())
        .spawn()
        .expect("the binary starts")
}

/// Waits for the `listening` line in `stderr` and returns every line up to
/// it; fails if `recovery_failed` arrives instead, or nothing does.
async fn until_listening(stderr: &Path) -> Vec<String> {
    for _ in 0..500 {
        let text = std::fs::read_to_string(stderr).unwrap_or_default();
        let lines: Vec<String> = text.lines().map(str::to_owned).collect();
        assert!(
            !text.contains("\"evt\":\"recovery_failed\""),
            "the node refused to start: {text}"
        );
        if let Some(at) = lines
            .iter()
            .position(|line| line.contains("\"evt\":\"listening\""))
        {
            return lines[..=at].to_vec();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("no listening line within ten seconds")
}

/// The port a `listening` line reports.
fn port_of(listening: &str) -> u16 {
    let after = listening.split("\"port\":").nth(1).expect("a port field");
    let digits: String = after.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().expect("a port number")
}

/// Starts the binary over `dir` as its data directory and returns the child
/// and its port.
async fn start(dir: &Path, stderr: &Path) -> (Child, u16) {
    let child = spawn(dir, &["--data-dir".as_ref(), dir.as_os_str()], stderr);
    let lines = until_listening(stderr).await;
    let port = port_of(lines.last().unwrap());
    (child, port)
}

async fn round_trip(stream: &mut TcpStream, parts: &[&str]) -> Frame {
    let frame = Frame::Array(
        parts
            .iter()
            .map(|part| Frame::Bulk(part.as_bytes().to_vec().into()))
            .collect(),
    );
    let mut out = Vec::new();
    encode(&frame, &mut out);
    stream.write_all(&out).await.unwrap();
    stream.flush().await.unwrap();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let got = stream.read(&mut chunk).await.unwrap();
        assert!(got > 0, "the server closed the connection");
        buf.extend_from_slice(&chunk[..got]);
        if let Some((frame, _)) = parse(&buf).expect("a well-formed reply") {
            return frame;
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_killed_node_serves_what_it_synced_when_it_is_started_again() {
    let dir = scratch();
    let stderr = dir.with_extension("stderr");
    let (mut first, port) = start(&dir, &stderr).await;
    {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        assert_eq!(
            round_trip(&mut stream, &["SET", "plain", "value"]).await,
            Frame::Simple("OK".into())
        );
        assert_eq!(
            round_trip(&mut stream, &["SET", "timed", "value", "EX", "100"]).await,
            Frame::Simple("OK".into())
        );
        assert_eq!(
            round_trip(&mut stream, &["INCRBY", "counter", "5"]).await,
            Frame::Integer(5)
        );
        assert_eq!(
            round_trip(&mut stream, &["SET", "doomed", "value"]).await,
            Frame::Simple("OK".into())
        );
        assert_eq!(
            round_trip(&mut stream, &["DEL", "doomed"]).await,
            Frame::Integer(1)
        );
    }
    // Several housekeeping ticks: the log is synced once every 100 ms.
    tokio::time::sleep(Duration::from_millis(500)).await;
    first.kill().expect("SIGKILL");
    first.wait().expect("reaped");

    let (mut second, port) = start(&dir, &stderr).await;
    let recovery = std::fs::read_to_string(&stderr).unwrap();
    assert!(
        recovery.contains("\"evt\":\"recovery\"") && recovery.contains("\"applied\":5"),
        "five records replayed: {recovery}"
    );
    assert!(
        ["holes", "abandoned_segments", "malformed", "lossy_shards"]
            .iter()
            .all(|field| recovery.contains(&format!("\"{field}\":"))),
        "the line says every way recovery can have cost records: {recovery}"
    );
    {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        assert_eq!(
            round_trip(&mut stream, &["GET", "plain"]).await,
            Frame::Bulk("value".into())
        );
        assert_eq!(
            round_trip(&mut stream, &["GET", "counter"]).await,
            Frame::Bulk("5".into())
        );
        assert_eq!(
            round_trip(&mut stream, &["GET", "doomed"]).await,
            Frame::Null
        );
        let Frame::Integer(ttl) = round_trip(&mut stream, &["TTL", "timed"]).await else {
            panic!("TTL answers an integer")
        };
        assert!(
            (90..=100).contains(&ttl),
            "the deadline survived as an absolute one: {ttl}"
        );
    }
    second.kill().expect("SIGKILL");
    second.wait().expect("reaped");
    std::fs::remove_dir_all(&dir).unwrap();
    std::fs::remove_file(&stderr).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_without_the_flag_writes_no_log() {
    let dir = scratch();
    let stderr = dir.with_extension("stderr");
    let mut child = spawn(&dir, &[], &stderr);
    let lines = until_listening(&stderr).await;
    {
        let port = port_of(lines.last().unwrap());
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        assert_eq!(
            round_trip(&mut stream, &["SET", "k", "v"]).await,
            Frame::Simple("OK".into())
        );
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    child.kill().unwrap();
    child.wait().unwrap();
    assert_eq!(
        lines.len(),
        1,
        "no recovery line without --data-dir: {lines:?}"
    );
    assert_eq!(
        std::fs::read_dir(&dir).unwrap().count(),
        0,
        "nothing was written in the working directory"
    );
    std::fs::remove_dir_all(&dir).unwrap();
    std::fs::remove_file(&stderr).unwrap();
}

/// Two processes on one data directory would each take a generation and
/// interleave two histories into one log. The second is refused before it
/// reads a byte, and says why; the first keeps serving.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_process_on_the_same_directory_is_refused() {
    let dir = scratch();
    let (mut first, _) = start(&dir, &dir.join("first.err")).await;

    let second_err = dir.join("second.err");
    let mut second = spawn(&dir, &["--data-dir".as_ref(), dir.as_os_str()], &second_err);
    let status = {
        let mut waited = None;
        for _ in 0..500 {
            if let Some(status) = second.try_wait().expect("try_wait") {
                waited = Some(status);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        waited
    };
    let text = std::fs::read_to_string(&second_err).unwrap_or_default();
    if status.is_none() {
        second.kill().ok();
    }
    first.kill().ok();
    first.wait().ok();
    assert_eq!(
        status.and_then(|status| status.code()),
        Some(1),
        "the second process exits 1: {text}"
    );
    assert!(
        text.contains("\"evt\":\"recovery_failed\"") && text.contains("another process"),
        "and says the directory is in use: {text}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Waits until a line with `evt` appears in `stderr`, returning every line
/// so far; fails after ten seconds.
async fn until_event(stderr: &Path, evt: &str) -> Vec<String> {
    let needle = format!("\"evt\":\"{evt}\"");
    for _ in 0..500 {
        let text = std::fs::read_to_string(stderr).unwrap_or_default();
        if text.contains(&needle) {
            return text.lines().map(str::to_owned).collect();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("no {evt} line within ten seconds")
}

/// The number `name` holds in the line `line`.
fn field(line: &str, name: &str) -> u64 {
    let start = line
        .find(&format!("\"{name}\":"))
        .unwrap_or_else(|| panic!("no {name} in {line}"))
        + name.len()
        + 3;
    line[start..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .unwrap_or_else(|_| panic!("{name} is not a number in {line}"))
}

/// Bytes of every file under `dir/wal`.
fn wal_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir.join("wal"))
        .unwrap()
        .map(|entry| entry.unwrap().metadata().unwrap().len())
        .sum()
}

/// `count` keys that all live on the node's first shard, and so on its
/// first executor whatever the number of cores.
///
/// The log is kept per executor, and so is the floor a snapshot waits for:
/// spread over every executor, the writes below would leave each one's log
/// far short of it. A key's shard is its CRC16 modulo the binary's 1024
/// shards, so a CRC that 1024 divides is shard 0, which every partition
/// gives to executor 0.
fn keys_on_one_executor(count: usize) -> Vec<String> {
    (0u64..)
        .map(|i| format!("k{i}"))
        .filter(|key| seedstone_core::slot::crc16_xmodem(key.as_bytes()).is_multiple_of(1024))
        .take(count)
        .collect()
}

/// Writes `total` bytes of values over `keys`, `value_len` each, in turn.
async fn write_past(stream: &mut TcpStream, total: u64, keys: &[String], value_len: usize) {
    let value = "v".repeat(value_len);
    let mut written = 0u64;
    for key in keys.iter().cycle() {
        if written >= total {
            break;
        }
        assert_eq!(
            round_trip(stream, &["SET", key, &value]).await,
            Frame::Simple("OK".into())
        );
        written += value_len as u64;
    }
}

/// Past the floor the node snapshots, the directory shrinks, and a restart
/// serves the keyspace from the image plus the tail.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_past_the_floor_snapshots_compacts_and_restarts_from_the_image() {
    let dir = scratch();
    let stderr = dir.with_extension("stderr");
    let (mut first, port) = start(&dir, &stderr).await;
    let floor = seedstone_core::log::checkpoint::SNAPSHOT_FLOOR;
    let keys = keys_on_one_executor(100);
    {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        // 100 keys of 16 KiB: an image of 1.6 MiB, a log that crosses the
        // floor after ~4100 writes, and 20 % more to leave a tail.
        write_past(&mut stream, floor + floor / 5, &keys, 16 * 1024).await;
        assert_eq!(
            round_trip(&mut stream, &["SET", "after", "tail"]).await,
            Frame::Simple("OK".into())
        );
    }
    let lines = until_event(&stderr, "compaction").await;
    let snapshot = lines
        .iter()
        .find(|line| line.contains("\"evt\":\"snapshot\""))
        .expect("a snapshot line precedes the compaction");
    assert_eq!(field(snapshot, "executor"), 0, "{snapshot}");
    assert_eq!(field(snapshot, "entries"), 100, "{snapshot}");
    assert!(field(snapshot, "disk_bytes") >= floor, "{snapshot}");
    let compaction = lines
        .iter()
        .find(|line| line.contains("\"evt\":\"compaction\""))
        .unwrap();
    assert!(field(compaction, "bytes") >= floor, "{compaction}");
    let after = wal_bytes(&dir);
    assert!(
        after < floor / 2,
        "the directory shrank below the floor once the snapshot covered the log: {after} bytes"
    );
    // The tail since the snapshot began is on disk before the kill.
    tokio::time::sleep(Duration::from_millis(500)).await;
    first.kill().expect("SIGKILL");
    first.wait().expect("reaped");

    let (mut second, port) = start(&dir, &stderr).await;
    let text = std::fs::read_to_string(&stderr).unwrap();
    let recovery = text
        .lines()
        .find(|line| line.contains("\"evt\":\"recovery\""))
        .unwrap();
    assert_eq!(field(recovery, "snapshots_used"), 1, "{recovery}");
    assert_eq!(field(recovery, "lossy_shards"), 0, "{recovery}");
    {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        assert_eq!(
            round_trip(&mut stream, &["DBSIZE"]).await,
            Frame::Integer(101),
            "100 keys from image plus tail, and the one written after"
        );
        assert_eq!(
            round_trip(&mut stream, &["GET", "after"]).await,
            Frame::Bulk("tail".into())
        );
        let Frame::Bulk(value) = round_trip(&mut stream, &["GET", &keys[7]]).await else {
            panic!("{} holds a value", keys[7])
        };
        assert_eq!(value.len(), 16 * 1024);
    }
    second.kill().expect("SIGKILL");
    second.wait().expect("reaped");
    std::fs::remove_dir_all(&dir).unwrap();
    std::fs::remove_file(&stderr).unwrap();
}

/// Killed the instant its snapshot line appears — with the compaction
/// possibly half done — the node starts again clean.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_killed_at_its_snapshot_line_starts_again_clean() {
    let dir = scratch();
    let stderr = dir.with_extension("stderr");
    let (mut first, port) = start(&dir, &stderr).await;
    let floor = seedstone_core::log::checkpoint::SNAPSHOT_FLOOR;
    let keys = keys_on_one_executor(50);
    {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        write_past(&mut stream, floor + floor / 10, &keys, 16 * 1024).await;
    }
    until_event(&stderr, "snapshot").await;
    first.kill().expect("SIGKILL");
    first.wait().expect("reaped");

    let (mut second, port) = start(&dir, &stderr).await;
    let text = std::fs::read_to_string(&stderr).unwrap();
    assert!(!text.contains("\"evt\":\"recovery_failed\""), "{text}");
    let recovery = text
        .lines()
        .find(|line| line.contains("\"evt\":\"recovery\""))
        .unwrap();
    assert_eq!(field(recovery, "lossy_shards"), 0, "{recovery}");
    {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        assert_eq!(
            round_trip(&mut stream, &["DBSIZE"]).await,
            Frame::Integer(50)
        );
        let Frame::Bulk(value) = round_trip(&mut stream, &["GET", &keys[0]]).await else {
            panic!("{} holds a value", keys[0])
        };
        assert_eq!(value.len(), 16 * 1024);
    }
    second.kill().expect("SIGKILL");
    second.wait().expect("reaped");
    std::fs::remove_dir_all(&dir).unwrap();
    std::fs::remove_file(&stderr).unwrap();
}

/// Starts the binary over `dir` as its data directory with `extra` after
/// the flag, and returns the child and its port.
async fn start_with(dir: &Path, stderr: &Path, extra: &[&str]) -> (Child, u16) {
    let mut args: Vec<&std::ffi::OsStr> = vec!["--data-dir".as_ref(), dir.as_os_str()];
    args.extend(extra.iter().map(|arg| std::ffi::OsStr::new(*arg)));
    let child = spawn(dir, &args, stderr);
    let lines = until_listening(stderr).await;
    let port = port_of(lines.last().unwrap());
    (child, port)
}

/// Writes every command in `commands` before reading a reply, then reads as
/// many replies as there were commands, in the order they arrive.
async fn pipelined(stream: &mut TcpStream, commands: &[&[&str]]) -> Vec<Frame> {
    let mut out = Vec::new();
    for parts in commands {
        let frame = Frame::Array(
            parts
                .iter()
                .map(|part| Frame::Bulk(part.as_bytes().to_vec().into()))
                .collect(),
        );
        encode(&frame, &mut out);
    }
    stream.write_all(&out).await.unwrap();
    stream.flush().await.unwrap();
    let mut replies = Vec::new();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    while replies.len() < commands.len() {
        if let Some((frame, used)) = parse(&buf).expect("a well-formed reply") {
            buf.drain(..used);
            replies.push(frame);
            continue;
        }
        let got = stream.read(&mut chunk).await.unwrap();
        assert!(got > 0, "the server closed the connection");
        buf.extend_from_slice(&chunk[..got]);
    }
    replies
}

/// Sends `SIGTERM` to `child` and reaps it.
fn terminate_and_wait(mut child: Child) {
    let status = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .expect("kill runs");
    assert!(status.success(), "kill -TERM was delivered");
    child.wait().expect("reaped");
}

async fn set_many(port: u16, count: usize) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    for i in 0..count {
        assert_eq!(
            round_trip(&mut stream, &["SET", &format!("k{i}"), "v"]).await,
            Frame::Simple("OK".into())
        );
    }
}

async fn dbsize(port: u16) -> Frame {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    round_trip(&mut stream, &["DBSIZE"]).await
}

/// Under `--fsync always`, a node killed right after its acknowledgements
/// serves every one of them when it is started again — no tick between.
///
/// A `SIGKILL` keeps the kernel's page cache, so this pins that every
/// acknowledged write reached the file and that recovery reads all of it;
/// that the bytes reached the device before the acknowledgement is the
/// simulator's to show, where a crash discards what no sync covered.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_killed_right_after_acknowledging_under_always_keeps_every_write() {
    let dir = scratch();
    let stderr = dir.with_extension("stderr");
    let (mut first, port) = start_with(&dir, &stderr, &["--fsync", "always"]).await;
    set_many(port, 200).await;
    first.kill().expect("SIGKILL");
    first.wait().expect("reaped");
    let (mut second, port) = start_with(&dir, &stderr, &["--fsync", "always"]).await;
    assert_eq!(dbsize(port).await, Frame::Integer(200));
    second.kill().expect("SIGKILL");
    second.wait().expect("reaped");
    std::fs::remove_dir_all(&dir).unwrap();
    std::fs::remove_file(&stderr).unwrap();
}

/// A pipelined `SET` then `GET` of one key under `always`: the `GET` runs
/// at once against memory, yet its reply lands after the `SET`'s, which
/// waits for its sync, and carries the value.
#[tokio::test(flavor = "multi_thread")]
async fn a_pipelined_read_behind_a_held_write_lands_after_it() {
    let dir = scratch();
    let stderr = dir.with_extension("stderr");
    let (mut node, port) = start_with(&dir, &stderr, &["--fsync", "always"]).await;
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let replies = pipelined(&mut stream, &[&["SET", "k", "v"], &["GET", "k"]]).await;
    assert_eq!(
        replies,
        vec![Frame::Simple("OK".into()), Frame::Bulk("v".into())]
    );
    node.kill().expect("SIGKILL");
    node.wait().expect("reaped");
    std::fs::remove_dir_all(&dir).unwrap();
    std::fs::remove_file(&stderr).unwrap();
}

/// Under `--fsync never` a clean stop loses nothing: a `SIGTERM` after the
/// writes, the stop completes within its grace period, and the next start
/// serves all of them.
///
/// As above, the page cache outlives the process; that the stop's final
/// sync covers what was flushed is the core's to show.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_stopped_cleanly_under_never_keeps_every_write() {
    let dir = scratch();
    let stderr = dir.with_extension("stderr");
    let (first, port) = start_with(&dir, &stderr, &["--fsync", "never"]).await;
    set_many(port, 200).await;
    terminate_and_wait(first);
    let stopped = std::fs::read_to_string(&stderr).unwrap();
    assert!(stopped.contains("\"evt\":\"stopping\""), "{stopped}");
    assert!(
        !stopped.contains("\"evt\":\"shutdown_timeout\""),
        "{stopped}"
    );
    let (mut second, port) = start_with(&dir, &stderr, &["--fsync", "never"]).await;
    assert_eq!(dbsize(port).await, Frame::Integer(200));
    second.kill().expect("SIGKILL");
    second.wait().expect("reaped");
    std::fs::remove_dir_all(&dir).unwrap();
    std::fs::remove_file(&stderr).unwrap();
}

/// `--fsync` without `--data-dir` is accepted, and says it does nothing.
#[tokio::test(flavor = "multi_thread")]
async fn fsync_without_a_data_dir_warns_once() {
    let dir = scratch();
    let stderr = dir.with_extension("stderr");
    let mut node = spawn(&dir, &["--fsync".as_ref(), "always".as_ref()], &stderr);
    let lines = until_listening(&stderr).await;
    assert_eq!(
        lines
            .iter()
            .filter(|line| line.contains("\"evt\":\"fsync_ignored\""))
            .count(),
        1,
        "{lines:?}"
    );
    node.kill().expect("SIGKILL");
    node.wait().expect("reaped");
    std::fs::remove_dir_all(&dir).unwrap();
    std::fs::remove_file(&stderr).unwrap();
}
