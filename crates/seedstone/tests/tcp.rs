//! The edge over a real socket.
//!
//! Everything below this crate is exercised over duplex pipes and simulated
//! TCP; those prove the protocol and the concurrency. They cannot prove that a
//! kernel socket, a listener and a semaphore were wired together correctly,
//! which is what this file is for. Ephemeral ports throughout — a fixed port
//! is a test that fails on a machine already running something.

use seedstone::server::{Config, MAX_CLIENTS_REACHED, Server};
use seedstone_core::dict::DictSeed;
use seedstone_resp::{Frame, encode, parse};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::test(flavor = "multi_thread")]
async fn serves_get_set_over_real_tcp() {
    let addr = started(4).await;

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut out = Vec::new();
    encode(&req(&["SET", "k", "v"]), &mut out);
    encode(&req(&["GET", "k"]), &mut out);
    stream.write_all(&out).await.unwrap();
    stream.flush().await.unwrap();

    let frames = read_frames(&mut stream, 2).await;
    assert_eq!(frames[0], Frame::Simple("OK".into()));
    assert_eq!(frames[1], Frame::Bulk(b"v".to_vec()));
}

/// Two connections in sequence reach the same keyspace: the pool outlives the
/// connection that wrote, which is the whole point of it living on the server
/// rather than on the accept.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_connection_reads_what_the_first_wrote() {
    let addr = started(4).await;

    let mut first = TcpStream::connect(addr).await.unwrap();
    round_trip(&mut first, &["SET", "shared", "value"]).await;
    drop(first);

    let mut second = TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        round_trip(&mut second, &["GET", "shared"]).await,
        Frame::Bulk(b"value".to_vec())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn over_limit_connections_are_told_and_closed() {
    // One permit. The first connection takes it and keeps it: the round trip
    // below is what proves the connection is being served, so the permit is
    // certainly spent by the time the second one arrives. Without that proof
    // this test would race the accept loop and pass for the wrong reason.
    let addr = started(1).await;

    let mut holder = TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        round_trip(&mut holder, &["SET", "k", "v"]).await,
        Frame::Simple("OK".into())
    );

    let mut refused = TcpStream::connect(addr).await.unwrap();
    let frames = read_frames(&mut refused, 1).await;
    assert_eq!(frames[0], Frame::Error(MAX_CLIENTS_REACHED.to_owned()));
    assert_eq!(
        refused.read(&mut [0u8; 64]).await.unwrap(),
        0,
        "a refused connection must be closed, not left open"
    );

    // And the permit comes back: the holder leaves, and the next connection is
    // served rather than refused in its turn.
    drop(holder);
    let mut next = None;
    for _ in 0..100 {
        let mut candidate = TcpStream::connect(addr).await.unwrap();
        if round_trip(&mut candidate, &["GET", "k"]).await == Frame::Bulk(b"v".to_vec()) {
            next = Some(candidate);
            break;
        }
        // The holder's task had not yet dropped its permit. Yield and retry
        // rather than sleep: the permit is released by a task, not by time.
        tokio::task::yield_now().await;
    }
    assert!(
        next.is_some(),
        "the permit was never released after the connection closed"
    );
}

// --- helpers ---

/// Binds an ephemeral port, spawns the accept loop, returns the address.
async fn started(max_clients: usize) -> std::net::SocketAddr {
    let cfg = Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        max_clients,
    };
    let server = Server::bind(cfg, DictSeed { k0: 1, k1: 2 }).await.unwrap();
    let addr = server.local_addr();
    tokio::spawn(server.run());
    addr
}

/// Sends one command and reads its single reply.
async fn round_trip(stream: &mut TcpStream, parts: &[&str]) -> Frame {
    let mut out = Vec::new();
    encode(&req(parts), &mut out);
    stream.write_all(&out).await.unwrap();
    stream.flush().await.unwrap();
    read_frames(stream, 1).await.remove(0)
}

fn req(parts: &[&str]) -> Frame {
    Frame::Array(
        parts
            .iter()
            .map(|p| Frame::Bulk(p.as_bytes().to_vec()))
            .collect(),
    )
}

async fn read_frames<R: AsyncRead + Unpin>(r: &mut R, n: usize) -> Vec<Frame> {
    let (mut buf, mut chunk, mut frames) = (Vec::new(), [0u8; 1024], Vec::new());
    while frames.len() < n {
        while let Some((f, used)) = parse(&buf).unwrap() {
            frames.push(f);
            buf.drain(..used);
            if frames.len() == n {
                return frames;
            }
        }
        let got = r.read(&mut chunk).await.unwrap();
        assert_ne!(got, 0, "stream closed with {} of {n} frames", frames.len());
        buf.extend_from_slice(&chunk[..got]);
    }
    frames
}
