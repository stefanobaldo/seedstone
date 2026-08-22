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
use seedstone_service::{RUN_ID_HEX, Secret};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::test(flavor = "multi_thread")]
async fn serves_get_set_over_real_tcp() {
    let addr = started(4, None).await;

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
    let addr = started(4, None).await;

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
    let addr = started(1, None).await;

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

/// `INFO` reports facts about the node, and the node is only assembled here.
///
/// The port is the one the kernel chose, which no layer below the accept loop
/// can know, and `connected_clients` is a count only the accept loop maintains
/// — so this is the only place either can be wrong. The count has to come back
/// down on every way a connection can end, and the two below are the ways that
/// are not a return from the connection loop: a peer that says `QUIT` and a
/// peer that simply vanishes.
#[tokio::test(flavor = "multi_thread")]
async fn info_reports_the_bound_port_and_the_live_connection_count() {
    let addr = started(4, None).await;

    let mut observer = TcpStream::connect(addr).await.unwrap();
    assert!(
        info_text(&mut observer)
            .await
            .contains(&format!("tcp_port:{}", addr.port())),
        "INFO did not report the port the listener actually bound"
    );
    assert!(
        connected_clients(&mut observer).await >= 1,
        "the observer's own connection was not counted"
    );

    // A peer that leaves politely, and one that is simply dropped.
    let mut quitter = TcpStream::connect(addr).await.unwrap();
    let mut vanisher = TcpStream::connect(addr).await.unwrap();
    round_trip(&mut quitter, &["PING"]).await;
    round_trip(&mut vanisher, &["PING"]).await;
    assert!(
        connected_clients(&mut observer).await >= 3,
        "three connections were attached and INFO saw fewer"
    );

    assert_eq!(
        round_trip(&mut quitter, &["QUIT"]).await,
        Frame::Simple("OK".into())
    );
    drop(quitter);
    drop(vanisher);

    // The decrement happens on the connection's task, so it is a yield away
    // rather than an instant away — the same shape as the permit above.
    let mut settled = false;
    for _ in 0..100 {
        if connected_clients(&mut observer).await == 1 {
            settled = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        settled,
        "connected_clients never came back down: {}",
        info_text(&mut observer).await
    );
}

/// The connection counters are the accept loop's, and it is the only party
/// that can maintain either.
///
/// `total_connections_received` counts every connection this loop took off the
/// listener, and `rejected_connections` counts the ones it then had no permit
/// for — so the second is a subset of the first, and a refusal that was not
/// also counted as an arrival would leave a monitor with a refusal rate above
/// one. The setup is the one-permit setup of
/// `over_limit_connections_are_told_and_closed`, for the same reason: the
/// round trip proves the permit is spent before the refusal is provoked.
#[tokio::test(flavor = "multi_thread")]
async fn info_counts_the_connections_accepted_and_the_ones_refused() {
    let addr = started(1, None).await;

    let mut holder = TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        round_trip(&mut holder, &["PING"]).await,
        Frame::Simple("PONG".into())
    );
    assert_eq!(
        info_field(&mut holder, "rejected_connections").await,
        0,
        "nothing has been refused yet"
    );

    let mut refused = TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        read_frames(&mut refused, 1).await[0],
        Frame::Error(MAX_CLIENTS_REACHED.to_owned())
    );
    drop(refused);

    // Both counters move on the accept loop's task, so they are a yield away
    // rather than an instant away — the shape every assertion in this file
    // about that loop has.
    let mut settled = false;
    for _ in 0..100 {
        if info_field(&mut holder, "rejected_connections").await == 1 {
            settled = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(settled, "the refusal was never counted");
    assert_eq!(
        info_field(&mut holder, "total_connections_received").await,
        2,
        "both the served connection and the refused one arrived"
    );
}

/// The bytes read and written are counted where they cross the socket.
#[tokio::test(flavor = "multi_thread")]
async fn info_counts_the_bytes_the_connection_carried() {
    let addr = started(4, None).await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    round_trip(&mut stream, &["SET", "k", "0123456789"]).await;
    assert!(
        info_field(&mut stream, "total_net_input_bytes").await > 0,
        "the commands this connection sent were not counted"
    );
    assert!(
        info_field(&mut stream, "total_net_output_bytes").await > 0,
        "the replies it received were not counted"
    );
}

/// `used_memory` is the pool's figure, read through a real socket.
#[tokio::test(flavor = "multi_thread")]
async fn info_reports_used_memory_that_moves_with_the_keyspace() {
    let addr = started(4, None).await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let before = info_field(&mut stream, "used_memory").await;
    round_trip(&mut stream, &["SET", "k", "0123456789"]).await;
    let after = info_field(&mut stream, "used_memory").await;
    assert!(
        after > before,
        "a write did not move used_memory: {before} -> {after}"
    );
}

/// The gate over a kernel socket, which is the one place it has never been
/// exercised: everything below this crate authenticates over a duplex pipe.
#[tokio::test(flavor = "multi_thread")]
async fn a_password_is_required_over_a_real_socket() {
    let addr = started(4, Some("pw")).await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        round_trip(&mut stream, &["PING"]).await,
        Frame::Error("NOAUTH Authentication required.".into())
    );
    assert_eq!(
        round_trip(&mut stream, &["AUTH", "pw"]).await,
        Frame::Simple("OK".into())
    );
    assert_eq!(
        round_trip(&mut stream, &["PING"]).await,
        Frame::Simple("PONG".into())
    );
}

// --- helpers ---

/// Binds an ephemeral port, spawns the accept loop, returns the address.
///
/// `password` is what a connection must present, or `None` for a node that
/// asks for none — which is what every test here but one wants.
async fn started(max_clients: usize, password: Option<&str>) -> std::net::SocketAddr {
    let cfg = Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        max_clients,
        password: password.map(|pw| Secret::new(pw.as_bytes().to_vec())),
        ..Config::default()
    };
    let server = Server::bind(cfg, DictSeed { k0: 1, k1: 2 }, "t".repeat(RUN_ID_HEX))
        .await
        .unwrap();
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

/// The text of one `INFO` reply.
async fn info_text(stream: &mut TcpStream) -> String {
    match round_trip(stream, &["INFO"]).await {
        Frame::Bulk(bytes) => String::from_utf8(bytes).expect("INFO is not UTF-8"),
        other => panic!("INFO answered {other:?}"),
    }
}

/// What `INFO` currently says the numeric field `name` is.
async fn info_field(stream: &mut TcpStream, name: &str) -> u64 {
    let text = info_text(stream).await;
    let prefix = format!("{name}:");
    text.lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .unwrap_or_else(|| panic!("INFO has no {name}: {text:?}"))
        .parse()
        .unwrap_or_else(|_| panic!("{name} is not a number: {text:?}"))
}

/// What `INFO` currently says `connected_clients` is.
async fn connected_clients(stream: &mut TcpStream) -> u64 {
    info_field(stream, "connected_clients").await
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
