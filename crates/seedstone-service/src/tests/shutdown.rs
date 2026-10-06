//! `SHUTDOWN`: the clean stop asked for by a client, behind the
//! authentication gate.

use super::support::{connected_to, node_with_password, read_frames, req};
use crate::node::NodeInfo;
use seedstone_resp::{Frame, encode};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn encoded(parts: &[&str]) -> Vec<u8> {
    let mut out = Vec::new();
    encode(&req(parts), &mut out);
    out
}

/// The connection fires the node's stop and hangs up without a reply, as
/// Redis does; both modifiers are the same request.
#[tokio::test]
async fn shutdown_closes_the_connection_without_a_reply_and_fires_the_stop() {
    for form in [
        &["SHUTDOWN"][..],
        &["SHUTDOWN", "NOSAVE"],
        &["shutdown", "save"],
    ] {
        let node = NodeInfo::for_tests();
        let (mut r, mut w, _pool) = connected_to(16, node.clone());
        w.write_all(&encoded(form)).await.unwrap();
        w.flush().await.unwrap();
        let mut buf = [0u8; 16];
        assert_eq!(
            r.read(&mut buf).await.unwrap(),
            0,
            "{form:?}: closed with no reply"
        );
        tokio::time::timeout(Duration::from_secs(1), node.stop.notified())
            .await
            .unwrap_or_else(|_| panic!("{form:?} did not fire the stop"));
    }
}

/// Review of the gate: an unauthenticated client on a node with a password
/// is told `NOAUTH`, and the node keeps running.
#[tokio::test]
async fn shutdown_is_noauth_on_a_node_with_a_password_and_fires_nothing() {
    let node = node_with_password(b"pw");
    let (mut r, mut w, _pool) = connected_to(16, node.clone());
    w.write_all(&encoded(&["SHUTDOWN"])).await.unwrap();
    w.write_all(&encoded(&["PING"])).await.unwrap();
    let frames = read_frames(&mut r, 2).await;
    assert!(
        matches!(&frames[0], Frame::Error(text) if text.starts_with("NOAUTH")),
        "{frames:?}"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(200), node.stop.notified())
            .await
            .is_err(),
        "the stop fired for an unauthenticated client"
    );
}

/// What 6.2.24 and 8.10.1 answer to a modifier they do not know, or to two
/// at once, read on 2026-10-06: `ERR syntax error`. Nothing fires.
#[tokio::test]
async fn shutdown_with_an_unknown_modifier_is_a_syntax_error() {
    let node = NodeInfo::for_tests();
    let (mut r, mut w, _pool) = connected_to(16, node.clone());
    w.write_all(&encoded(&["SHUTDOWN", "bogus"])).await.unwrap();
    w.write_all(&encoded(&["SHUTDOWN", "NOSAVE", "SAVE"]))
        .await
        .unwrap();
    assert_eq!(
        read_frames(&mut r, 2).await,
        vec![
            Frame::Error("ERR syntax error".into()),
            Frame::Error("ERR syntax error".into()),
        ]
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), node.stop.notified())
            .await
            .is_err()
    );
}
