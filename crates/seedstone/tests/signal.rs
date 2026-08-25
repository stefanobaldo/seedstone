//! SIGTERM ends the accept loop the way Ctrl-C does.
//!
//! Its own test binary, deliberately: the signal is delivered to the whole
//! process, and a test binary that has not installed the handler yet dies of
//! it. Here the handler is installed by the server under test before the
//! signal is sent — the connection that succeeds first proves the loop has
//! polled its `select!` at least once.
#![cfg(unix)]

use seedstone::server::{Config, Server};
use seedstone_core::dict::DictSeed;
use seedstone_service::RUN_ID_HEX;
use tokio::net::TcpStream;

#[tokio::test(flavor = "multi_thread")]
async fn sigterm_ends_the_accept_loop() {
    let cfg = Config {
        bind: "127.0.0.1:0".parse().unwrap(),
        max_clients: 4,
        ..Config::default()
    };
    let server = Server::bind(cfg, DictSeed { k0: 1, k1: 2 }, "t".repeat(RUN_ID_HEX))
        .await
        .unwrap();
    let addr = server.local_addr();
    let running = tokio::spawn(server.run());

    // The loop is serving: it has been polled, so its signal watchers exist.
    let _probe = TcpStream::connect(addr).await.unwrap();

    let status = std::process::Command::new("kill")
        .args(["-TERM", &std::process::id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());

    tokio::time::timeout(std::time::Duration::from_secs(5), running)
        .await
        .expect("the accept loop did not end within 5 s of SIGTERM")
        .unwrap();
}
