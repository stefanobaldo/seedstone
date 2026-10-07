//! The binary, killed and restarted on the same directory, serves what it
//! acknowledged before the last sync.
//!
//! The one test in the tree that runs the release path end to end: a real
//! process, a real `SIGKILL`, a real directory. Everything below this is
//! exercised on an in-memory disk or the simulator's; this is what says the
//! `std::fs` half of the seam was wired into the composition root.

use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

/// A running binary that is killed and reaped when it is dropped, so a test
/// that fails before its own `kill` does not leave a server behind.
struct Node(Child);

impl Deref for Node {
    type Target = Child;

    fn deref(&self) -> &Child {
        &self.0
    }
}

impl DerefMut for Node {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.0
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        // Either fails harmlessly on a child the test already reaped.
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Starts the binary on an ephemeral port, in `cwd`, with `args` after the
/// bind, its stderr written to `stderr`.
///
/// Stderr goes to a file rather than a pipe: a pipe must be drained for as
/// long as the child lives, and a file is also there to read when an
/// assertion fails.
fn spawn(cwd: &Path, args: &[&std::ffi::OsStr], stderr: &Path) -> Node {
    Node(
        Command::new(env!("CARGO_BIN_EXE_seedstone"))
            .args(["--bind", "127.0.0.1:0"])
            .args(args)
            .current_dir(cwd)
            .stderr(std::fs::File::create(stderr).unwrap())
            .stdout(Stdio::null())
            .spawn()
            .expect("the binary starts"),
    )
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
async fn start(dir: &Path, stderr: &Path) -> (Node, u16) {
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
    assert!(field(compaction, "files") >= 1, "{compaction}");
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
async fn start_with(dir: &Path, stderr: &Path, extra: &[&str]) -> (Node, u16) {
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
fn terminate_and_wait(mut child: Node) {
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

/// CRC-32/ISO-HDLC, bit by bit: the checksum a segment header carries.
fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

/// A directory written by a build whose segments named their executor is
/// refused at start, with a line that says what it is — not read as damage
/// to every shard and served empty.
#[tokio::test(flavor = "multi_thread")]
async fn a_directory_from_the_previous_format_is_refused_with_a_line_that_says_so() {
    let dir = scratch();
    let wal = dir.join("wal");
    std::fs::create_dir_all(&wal).unwrap();
    // The previous layout's header: magic, version 1, generation, executor,
    // rotation, checksum.
    let mut header = Vec::new();
    header.extend_from_slice(b"SSEG");
    header.push(1);
    header.extend_from_slice(&1u64.to_le_bytes());
    header.extend_from_slice(&0u16.to_le_bytes());
    header.extend_from_slice(&0u32.to_le_bytes());
    let crc = crc32(&header);
    header.extend_from_slice(&crc.to_le_bytes());
    std::fs::write(wal.join("0000000000000001-0000-00000000.seg"), header).unwrap();
    let stderr = dir.join("stderr");
    let mut child = spawn(&dir, &["--data-dir".as_ref(), dir.as_os_str()], &stderr);
    let status = child.wait().expect("reaped");
    let lines = std::fs::read_to_string(&stderr).unwrap();
    assert_eq!(status.code(), Some(1), "{lines}");
    assert!(
        lines.contains("\"evt\":\"recovery_failed\"") && lines.contains("predates"),
        "{lines}"
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

/// `INFO persistence` on a node with a log: the log is on, the policy is
/// named, the write counts as a change, and no image exists yet — so the
/// time of the last one is absent rather than zero.
#[tokio::test(flavor = "multi_thread")]
async fn info_persistence_reports_the_log_and_the_images() {
    let dir = scratch();
    let stderr = dir.with_extension("stderr");
    let (mut node, port) = start_with(&dir, &stderr, &["--fsync", "interval"]).await;
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    assert_eq!(
        round_trip(&mut stream, &["SET", "k", "v"]).await,
        Frame::Simple("OK".into())
    );
    let Frame::Bulk(text) = round_trip(&mut stream, &["INFO", "persistence"]).await else {
        panic!("INFO answers a bulk")
    };
    let text = String::from_utf8(text.to_vec()).unwrap();
    assert!(text.contains("aof_enabled:1\r\n"), "{text}");
    assert!(text.contains("fsync_policy:interval\r\n"), "{text}");
    assert!(text.contains("rdb_changes_since_last_save:1\r\n"), "{text}");
    assert!(text.contains("rdb_saves:0\r\n"), "{text}");
    assert!(text.contains("log_segments:1\r\n"), "{text}");
    assert!(!text.contains("rdb_last_save_time:"), "{text}");
    node.kill().expect("SIGKILL");
    node.wait().expect("reaped");
    std::fs::remove_dir_all(&dir).unwrap();
    std::fs::remove_file(&stderr).unwrap();
}

/// The executors the binary runs on this host.
fn available_parallelism() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}

/// Names of the segments and snapshots under `dir/wal`.
fn log_names(dir: &Path) -> Vec<String> {
    std::fs::read_dir(dir.join("wal"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .filter(|name| {
            Path::new(name)
                .extension()
                .is_some_and(|ext| ext == "seg" || ext == "snap")
        })
        .collect()
}

/// Lines with `evt` in `stderr`, once there are at least `count` of them;
/// fails after thirty seconds.
async fn until_event_count(stderr: &Path, evt: &str, count: usize) -> Vec<String> {
    let needle = format!("\"evt\":\"{evt}\"");
    for _ in 0..1500 {
        let text = std::fs::read_to_string(stderr).unwrap_or_default();
        let found: Vec<String> = text
            .lines()
            .filter(|line| line.contains(&needle))
            .map(str::to_owned)
            .collect();
        if found.len() >= count {
            return found;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "fewer than {count} {evt} lines within thirty seconds: {}",
        std::fs::read_to_string(stderr).unwrap_or_default()
    )
}

/// Writes 16 KiB values over keys spread across every shard, and so across
/// every executor, until `total` bytes of values are written or a reply is
/// not `OK`; returns that reply, if one came.
async fn write_spread(port: u16, total: u64) -> Option<Frame> {
    const VALUE: usize = 16 * 1024;
    let value = "v".repeat(VALUE);
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    for i in 0..total / VALUE as u64 {
        let reply = round_trip(&mut stream, &["SET", &format!("s{i}"), &value]).await;
        if reply != Frame::Simple("OK".into()) {
            return Some(reply);
        }
    }
    None
}

/// Spread over every executor, 70 MiB leaves each one's own writing far
/// short of the floor. Killed, then started again, the node holds the
/// previous process's files as retained log above the bound: every
/// executor is asked for a snapshot, and once each is durable the older
/// files go together.
#[tokio::test(flavor = "multi_thread")]
async fn after_a_restart_the_previous_process_s_files_are_replaced_once_they_exceed_the_bound() {
    let dir = scratch();
    let first_err = dir.with_extension("stderr-1");
    let (mut first, port) = start_with(&dir, &first_err, &["--fsync", "never"]).await;
    assert_eq!(write_spread(port, 70 * 1024 * 1024).await, None);
    first.kill().expect("SIGKILL");
    first.wait().expect("reaped");
    let older = log_names(&dir);
    assert!(!older.is_empty());

    let second_err = dir.with_extension("stderr-2");
    let (mut second, port) = start_with(&dir, &second_err, &["--fsync", "never"]).await;
    set_many(port, 10).await;
    until_event_count(&second_err, "snapshot", available_parallelism()).await;
    let mut left = older.clone();
    for _ in 0..1500 {
        left.retain(|name| dir.join("wal").join(name).exists());
        if left.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        left.is_empty(),
        "{left:?} stayed: {}",
        std::fs::read_to_string(&second_err).unwrap()
    );
    // Recovery removes a snapshot the kill left unfinished; the node, the
    // rest. The files go before the line that reports them is written.
    let removed = |text: &str| -> u64 {
        text.lines()
            .filter(|line| {
                line.contains("\"evt\":\"compaction\"") || line.contains("\"evt\":\"recovery\"")
            })
            .map(|line| {
                if line.contains("\"evt\":\"recovery\"") {
                    field(line, "files_removed")
                } else {
                    field(line, "files")
                }
            })
            .sum()
    };
    let mut text = String::new();
    for _ in 0..500 {
        text = std::fs::read_to_string(&second_err).unwrap();
        if removed(&text) >= older.len() as u64 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(removed(&text) >= older.len() as u64, "{older:?}: {text}");
    second.kill().expect("SIGKILL");
    second.wait().expect("reaped");
    std::fs::remove_dir_all(&dir).unwrap();
    std::fs::remove_file(&first_err).unwrap();
    std::fs::remove_file(&second_err).unwrap();
}

/// A log that cannot rotate is one `log_fault` line for the node; every
/// executor refuses writes, and each returns on its own `refusal_ended`
/// line once its snapshot is durable.
///
/// The directory is made read-only after the start: the open segment keeps
/// taking writes, and the rotation past 64 MiB cannot create its file —
/// nor can a snapshot until the directory is writable again. A process
/// that ignores the permission (root) cannot be made to fail this way, and
/// the test says so and stops.
#[tokio::test(flavor = "multi_thread")]
async fn a_log_that_cannot_rotate_is_one_line_for_the_node_and_one_return_per_executor() {
    use std::os::unix::fs::PermissionsExt;
    let dir = scratch();
    let stderr = dir.with_extension("stderr");
    let (mut node, port) = start_with(&dir, &stderr, &["--fsync", "never"]).await;
    let wal = dir.join("wal");
    std::fs::set_permissions(&wal, std::fs::Permissions::from_mode(0o555)).unwrap();
    if std::fs::write(wal.join("probe"), b"").is_ok() {
        eprintln!("this process writes to a read-only directory; nothing to provoke");
        node.kill().expect("SIGKILL");
        node.wait().expect("reaped");
        std::fs::remove_dir_all(&dir).unwrap();
        return;
    }
    let refused = write_spread(port, 80 * 1024 * 1024).await;
    let Some(Frame::Error(text)) = refused else {
        panic!("no refusal within 80 MiB: {refused:?}")
    };
    assert!(text.starts_with("MISCONF"), "{text}");
    std::fs::set_permissions(&wal, std::fs::Permissions::from_mode(0o755)).unwrap();
    let ended = until_event_count(&stderr, "refusal_ended", available_parallelism()).await;
    assert_eq!(ended.len(), available_parallelism(), "{ended:?}");
    set_many(port, 10).await;
    let text = std::fs::read_to_string(&stderr).unwrap();
    let faults: Vec<&str> = text
        .lines()
        .filter(|line| line.contains("\"evt\":\"log_fault\""))
        .collect();
    assert_eq!(faults.len(), 1, "{text}");
    assert!(faults[0].contains("\"stage\":\"rotate\""), "{}", faults[0]);
    assert!(!faults[0].contains("\"shard\""), "{}", faults[0]);
    node.kill().expect("SIGKILL");
    node.wait().expect("reaped");
    std::fs::remove_dir_all(&dir).unwrap();
    std::fs::remove_file(&stderr).unwrap();
}

/// `SHUTDOWN` from a client is the clean stop: the process exits with
/// success, says `stopping` with `SHUTDOWN` as its signal, and under
/// `never` the log it synced on the way out holds the write.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_from_a_client_is_the_clean_stop() {
    let dir = scratch();
    let stderr = dir.with_extension("stderr");
    let (mut node, port) = start_with(&dir, &stderr, &["--fsync", "never"]).await;
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    assert_eq!(
        round_trip(&mut stream, &["SET", "k", "v"]).await,
        Frame::Simple("OK".into())
    );
    let mut out = Vec::new();
    encode(
        &Frame::Array(vec![Frame::Bulk("SHUTDOWN".into())]),
        &mut out,
    );
    stream.write_all(&out).await.unwrap();
    let mut buf = [0u8; 16];
    assert_eq!(
        stream.read(&mut buf).await.unwrap(),
        0,
        "closed with no reply"
    );
    let status = node.wait().unwrap();
    assert!(status.success(), "{status}");
    let lines = std::fs::read_to_string(&stderr).unwrap();
    assert!(
        lines.contains(r#""evt":"stopping","signal":"SHUTDOWN"}"#),
        "{lines}"
    );
    assert!(!lines.contains("shutdown_timeout"), "{lines}");
    let (mut again, port) = start_with(&dir, &stderr, &["--fsync", "never"]).await;
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    assert_eq!(
        round_trip(&mut stream, &["GET", "k"]).await,
        Frame::Bulk("v".into()),
        "the clean stop synced the log"
    );
    again.kill().unwrap();
    again.wait().unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
    std::fs::remove_file(&stderr).unwrap();
}

/// `SAVE` under each policy: `OK` once every executor's image is durable,
/// `LASTSAVE` advances to it, and a restart starts from the images and
/// dates them the same.
#[tokio::test(flavor = "multi_thread")]
async fn save_lands_an_image_on_every_executor_and_lastsave_dates_it() {
    for policy in ["always", "interval", "never"] {
        let dir = scratch();
        let stderr = dir.with_extension("stderr");
        let (mut node, port) = start_with(&dir, &stderr, &["--fsync", policy]).await;
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        assert_eq!(
            round_trip(&mut stream, &["LASTSAVE"]).await,
            Frame::Integer(0),
            "{policy}"
        );
        set_many(port, 50).await;
        assert_eq!(
            round_trip(&mut stream, &["SAVE"]).await,
            Frame::Simple("OK".into()),
            "{policy}"
        );
        let snapshots = until_event_count(&stderr, "snapshot", available_parallelism()).await;
        assert_eq!(
            snapshots.len(),
            available_parallelism(),
            "{policy}: one image per executor"
        );
        let Frame::Integer(at) = round_trip(&mut stream, &["LASTSAVE"]).await else {
            panic!("{policy}: LASTSAVE is an integer")
        };
        // The image is dated when its cycle opened, just before its file
        // was written: within a second or two of the files' own times. The
        // filesystem's clock rather than the test's, which the determinism
        // lints keep out of the tree.
        let written = snapshot_mtimes(&dir);
        let (oldest, newest) = (written[0], written[written.len() - 1]);
        assert!(
            at > 0 && at <= newest && at + 2 >= oldest,
            "{policy}: LASTSAVE {at}, files written {oldest}..={newest}"
        );
        node.kill().unwrap();
        node.wait().unwrap();
        let (mut again, port) = start_with(&dir, &stderr, &["--fsync", policy]).await;
        let recovery = until_event(&stderr, "recovery").await;
        let line = recovery
            .iter()
            .find(|line| line.contains("\"evt\":\"recovery\""))
            .unwrap();
        assert_eq!(
            field(line, "snapshots_used"),
            available_parallelism() as u64,
            "{policy}: {line}"
        );
        // The start reports how long it spent with the directory before it
        // listened: a number, present on every start, small on fifty keys.
        assert!(
            field(line, "elapsed_ms") < 10_000,
            "{policy}: elapsed_ms is a number under ten seconds: {line}"
        );
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        assert_eq!(
            round_trip(&mut stream, &["LASTSAVE"]).await,
            Frame::Integer(at),
            "{policy}: LASTSAVE survives a restart"
        );
        assert_eq!(dbsize(port).await, Frame::Integer(50), "{policy}");
        again.kill().unwrap();
        again.wait().unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
        std::fs::remove_file(&stderr).unwrap();
    }
}

/// The modification times of `dir/wal`'s snapshot files, in Unix seconds,
/// ascending.
fn snapshot_mtimes(dir: &Path) -> Vec<i64> {
    let mut times: Vec<i64> = std::fs::read_dir(dir.join("wal"))
        .unwrap()
        .map(|entry| entry.unwrap())
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".snap"))
        .map(|entry| {
            let modified = entry.metadata().unwrap().modified().unwrap();
            let since = modified.duration_since(std::time::UNIX_EPOCH).unwrap();
            i64::try_from(since.as_secs()).unwrap()
        })
        .collect();
    times.sort_unstable();
    assert!(!times.is_empty(), "no snapshot file");
    times
}

/// `SAVE` pipelined behind writes under `always` is answered after them,
/// and a kill right after its `OK` loses none of them.
#[tokio::test(flavor = "multi_thread")]
async fn a_save_pipelined_behind_writes_under_always_is_answered_after_them_and_keeps_them() {
    let dir = scratch();
    let stderr = dir.with_extension("stderr");
    let (mut node, port) = start_with(&dir, &stderr, &["--fsync", "always"]).await;
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let replies = pipelined(
        &mut stream,
        &[&["SET", "a", "1"], &["SET", "b", "2"], &["SAVE"]],
    )
    .await;
    assert_eq!(replies, vec![Frame::Simple("OK".into()); 3]);
    node.kill().unwrap();
    node.wait().unwrap();
    let (mut again, port) = start_with(&dir, &stderr, &["--fsync", "always"]).await;
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    assert_eq!(
        round_trip(&mut stream, &["MGET", "a", "b"]).await,
        Frame::Array(vec![Frame::Bulk("1".into()), Frame::Bulk("2".into())])
    );
    again.kill().unwrap();
    again.wait().unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
    std::fs::remove_file(&stderr).unwrap();
}

/// Without `--data-dir`, `SAVE`, `BGSAVE` and `LASTSAVE` each name the flag.
#[tokio::test(flavor = "multi_thread")]
async fn without_a_data_dir_the_three_persistence_commands_name_the_flag() {
    let dir = scratch();
    let stderr = dir.with_extension("stderr");
    let mut node = spawn(&dir, &[], &stderr);
    let lines = until_listening(&stderr).await;
    let port = port_of(lines.last().unwrap());
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    for cmd in [&["SAVE"][..], &["BGSAVE"], &["LASTSAVE"]] {
        let Frame::Error(text) = round_trip(&mut stream, cmd).await else {
            panic!("{cmd:?} is an error")
        };
        assert!(text.contains("--data-dir"), "{text}");
    }
    node.kill().unwrap();
    node.wait().unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
    std::fs::remove_file(&stderr).unwrap();
}

/// `conns` connections each pipelining `depth` `SET`s of `value_len` bytes
/// without pause until `stop` is set, as full an inbox as clients can keep. Each
/// connection cycles over a thousand keys of its own, so the flood grows
/// the log and not the keyspace.
fn flood(
    port: u16,
    conns: usize,
    depth: usize,
    value_len: usize,
    stop: &Arc<AtomicBool>,
) -> Vec<tokio::task::JoinHandle<u64>> {
    (0..conns)
        .map(|c| {
            let stop = stop.clone();
            tokio::spawn(async move {
                let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
                let value = "x".repeat(value_len);
                let mut sent = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let keys: Vec<String> = (0..depth as u64)
                        .map(|i| format!("f{c}-{}", (sent + i) % 1000))
                        .collect();
                    let commands: Vec<[&str; 3]> =
                        keys.iter().map(|key| ["SET", key, &value]).collect();
                    let refs: Vec<&[&str]> = commands.iter().map(<[&str; 3]>::as_slice).collect();
                    pipelined(&mut stream, &refs).await;
                    sent += depth as u64;
                }
                sent
            })
        })
        .collect()
}

/// How many lines carry `evt` in `stderr` by the time `count` of them are
/// there or `within` has passed, whichever is first.
async fn event_count_within(stderr: &Path, evt: &str, count: usize, within: Duration) -> usize {
    let needle = format!("\"evt\":\"{evt}\"");
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let text = std::fs::read_to_string(stderr).unwrap_or_default();
        let found = text.lines().filter(|line| line.contains(&needle)).count();
        if found >= count || tokio::time::Instant::now() >= deadline {
            return found;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A refusal ends while sixteen connections keep pipelining writes at it:
/// the housekeeping tick that lands the image is not starved by the inbox.
#[tokio::test(flavor = "multi_thread")]
async fn a_refusal_ends_under_a_flood_of_writes() {
    use std::os::unix::fs::PermissionsExt;
    let dir = scratch();
    let stderr = dir.with_extension("stderr");
    let (mut node, port) = start_with(&dir, &stderr, &["--fsync", "never"]).await;
    let wal = dir.join("wal");
    std::fs::set_permissions(&wal, std::fs::Permissions::from_mode(0o555)).unwrap();
    if std::fs::write(wal.join("probe"), b"").is_ok() {
        eprintln!("this process writes to a read-only directory; nothing to provoke");
        node.kill().expect("SIGKILL");
        node.wait().expect("reaped");
        std::fs::remove_dir_all(&dir).unwrap();
        return;
    }
    let refused = write_spread(port, 80 * 1024 * 1024).await;
    assert!(matches!(refused, Some(Frame::Error(_))), "{refused:?}");
    let stop = Arc::new(AtomicBool::new(false));
    let tasks = flood(port, 16, 128, 64, &stop);
    tokio::time::sleep(Duration::from_millis(500)).await;
    std::fs::set_permissions(&wal, std::fs::Permissions::from_mode(0o755)).unwrap();
    let executors = available_parallelism();
    let ended =
        event_count_within(&stderr, "refusal_ended", executors, Duration::from_secs(20)).await;
    stop.store(true, Ordering::Relaxed);
    for task in tasks {
        task.await.unwrap();
    }
    node.kill().expect("SIGKILL");
    node.wait().expect("reaped");
    assert_eq!(
        ended, executors,
        "{ended} of {executors} refusals ended under the flood within 20 s"
    );
    std::fs::remove_dir_all(&dir).unwrap();
    std::fs::remove_file(&stderr).unwrap();
}

/// The log compacts while a flood of writes runs, not only after it.
#[tokio::test(flavor = "multi_thread")]
async fn the_log_compacts_under_a_flood_of_writes() {
    let dir = scratch();
    let stderr = dir.with_extension("stderr");
    let (mut node, port) = start_with(&dir, &stderr, &["--fsync", "never"]).await;
    let stop = Arc::new(AtomicBool::new(false));
    let tasks = flood(port, 16, 128, 4096, &stop);
    let compactions = event_count_within(&stderr, "compaction", 1, Duration::from_mins(1)).await;
    stop.store(true, Ordering::Relaxed);
    let mut sent = 0;
    for task in tasks {
        sent += task.await.unwrap();
    }
    node.kill().expect("SIGKILL");
    node.wait().expect("reaped");
    assert!(
        compactions > 0,
        "no compaction while the flood ran ({sent} writes sent)"
    );
    std::fs::remove_dir_all(&dir).unwrap();
    std::fs::remove_file(&stderr).unwrap();
}

/// A `SAVE` is answered while sixty-four connections keep walking the
/// keyspace with `KEYS`. A walk queues each step of its cursor the moment
/// the previous one returns, with no client round trip between them, so the
/// executors' inboxes need never run dry — and the housekeeping tick that
/// opens the asked-for image must run anyway (#79).
#[tokio::test(flavor = "multi_thread")]
async fn save_is_answered_while_the_keyspace_is_walked() {
    let dir = scratch();
    let stderr = dir.with_extension("stderr");
    let (mut node, port) = start_with(&dir, &stderr, &["--fsync", "never"]).await;
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let keys: Vec<String> = (0..200_000).map(|i| format!("w{i}")).collect();
    for chunk in keys.chunks(1000) {
        let commands: Vec<[&str; 3]> = chunk.iter().map(|key| ["SET", key, "v"]).collect();
        let refs: Vec<&[&str]> = commands.iter().map(<[&str; 3]>::as_slice).collect();
        pipelined(&mut stream, &refs).await;
    }
    let stop = Arc::new(AtomicBool::new(false));
    let walkers: Vec<_> = (0..64)
        .map(|_| {
            let stop = stop.clone();
            tokio::spawn(async move {
                let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
                while !stop.load(Ordering::Relaxed) {
                    round_trip(&mut stream, &["KEYS", "zz*"]).await;
                }
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let saved =
        tokio::time::timeout(Duration::from_secs(20), round_trip(&mut stream, &["SAVE"])).await;
    stop.store(true, Ordering::Relaxed);
    for walker in walkers {
        walker.await.unwrap();
    }
    node.kill().expect("SIGKILL");
    node.wait().expect("reaped");
    assert_eq!(
        saved.ok(),
        Some(Frame::Simple("OK".into())),
        "SAVE not answered within 20 s while the keyspace was walked"
    );
    std::fs::remove_dir_all(&dir).unwrap();
    std::fs::remove_file(&stderr).unwrap();
}

/// Writes pipelined ahead of `SHUTDOWN` on its own connection are applied
/// and synced before the stop: they were sent first, so they are served
/// first. Tried twenty times, since what it guards is an ordering between
/// the connection and the stop it asks for.
#[tokio::test(flavor = "multi_thread")]
async fn writes_pipelined_ahead_of_shutdown_are_kept() {
    const WRITES: i64 = 2000;
    for round in 0..20 {
        let dir = scratch();
        let stderr = dir.with_extension("stderr");
        let (mut node, port) = start_with(&dir, &stderr, &["--fsync", "never"]).await;
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let mut out = Vec::new();
        for i in 0..WRITES {
            let set = ["SET".to_owned(), format!("p{i}"), "v".to_owned()];
            encode(
                &Frame::Array(
                    set.into_iter()
                        .map(|part| Frame::Bulk(part.into()))
                        .collect(),
                ),
                &mut out,
            );
        }
        encode(
            &Frame::Array(vec![Frame::Bulk("SHUTDOWN".into())]),
            &mut out,
        );
        stream.write_all(&out).await.unwrap();
        let mut sink = Vec::new();
        let _ = stream.read_to_end(&mut sink).await;
        let status = node.wait().unwrap();
        assert!(status.success(), "round {round}: {status}");
        let (mut again, port) = start_with(&dir, &stderr, &["--fsync", "never"]).await;
        let kept = dbsize(port).await;
        again.kill().unwrap();
        again.wait().unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
        std::fs::remove_file(&stderr).unwrap();
        assert_eq!(
            kept,
            Frame::Integer(WRITES),
            "round {round}: writes pipelined ahead of SHUTDOWN lost"
        );
    }
}

/// A test that fails between starting a node and killing it drops the
/// handle on its way out; the node goes with it rather than serving on.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_whose_handle_is_dropped_is_stopped() {
    let dir = scratch();
    let (node, _) = start(&dir, &dir.join("err")).await;
    let pid = node.id().to_string();
    drop(node);
    let alive = Command::new("kill")
        .args(["-0", &pid])
        .stderr(Stdio::null())
        .status()
        .expect("kill runs")
        .success();
    assert!(
        !alive,
        "process {pid} still runs after its handle was dropped"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
