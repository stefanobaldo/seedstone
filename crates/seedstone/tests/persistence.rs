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
