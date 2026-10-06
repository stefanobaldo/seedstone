//! `SAVE`, `BGSAVE` and `LASTSAVE` at the edge: the fold of every
//! executor's answer, and what a node without a log says instead.

use super::support::{connected, connected_with_log, read_frames, req};
use seedstone_resp::{Frame, encode};
use tokio::io::AsyncWriteExt;

fn encoded(requests: &[&[&str]]) -> Vec<u8> {
    let mut out = Vec::new();
    for parts in requests {
        encode(&req(parts), &mut out);
    }
    out
}

/// `BGSAVE` starts a snapshot; a second one right behind it finds every
/// executor already in one and gets Redis's refusal; `LASTSAVE` is `0`
/// until an image exists.
#[tokio::test]
async fn bgsave_starts_once_and_a_second_is_refused_while_it_runs() {
    let (mut r, mut w, _pool, dir) = connected_with_log(16);
    w.write_all(&encoded(&[&["LASTSAVE"], &["BGSAVE"], &["BGSAVE"]]))
        .await
        .unwrap();
    let frames = read_frames(&mut r, 3).await;
    assert_eq!(
        frames,
        vec![
            Frame::Integer(0),
            Frame::Simple("Background saving started".into()),
            Frame::Error("ERR Background save already in progress".into()),
        ]
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

/// `SAVE` is answered `OK` once every executor's image is durable, and
/// `LASTSAVE` then dates it.
#[tokio::test]
async fn save_answers_once_the_images_land_and_lastsave_dates_them() {
    let (mut r, mut w, pool, dir) = connected_with_log(16);
    w.write_all(&encoded(&[&["SET", "k", "v"], &["SAVE"], &["LASTSAVE"]]))
        .await
        .unwrap();
    let frames = read_frames(&mut r, 3).await;
    assert_eq!(
        frames[..2],
        [Frame::Simple("OK".into()), Frame::Simple("OK".into())]
    );
    assert!(
        matches!(frames[2], Frame::Integer(at) if at > 0),
        "{frames:?}"
    );
    assert_eq!(pool.stats().saves(), 4, "one image per executor");
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Without a log there is nothing to save or to date: each of the three
/// names the flag that would give the node one.
#[tokio::test]
async fn without_a_log_the_three_name_the_flag() {
    let (mut r, mut w, _pool) = connected(16);
    w.write_all(&encoded(&[&["SAVE"], &["BGSAVE"], &["LASTSAVE"]]))
        .await
        .unwrap();
    for frame in read_frames(&mut r, 3).await {
        assert!(
            matches!(&frame, Frame::Error(text) if text.contains("--data-dir")),
            "{frame:?}"
        );
    }
}

/// Arguments: what 6.2.24 and 8.10.1 answer, read on 2026-10-06.
#[tokio::test]
async fn their_arguments_are_refused_as_redis_refuses_them() {
    let (mut r, mut w, _pool, dir) = connected_with_log(16);
    w.write_all(&encoded(&[
        &["BGSAVE", "foo"],
        &["SAVE", "x"],
        &["LASTSAVE", "x"],
    ]))
    .await
    .unwrap();
    assert_eq!(
        read_frames(&mut r, 3).await,
        vec![
            Frame::Error("ERR syntax error".into()),
            Frame::Error("ERR wrong number of arguments for 'save' command".into()),
            Frame::Error("ERR wrong number of arguments for 'lastsave' command".into()),
        ]
    );
    std::fs::remove_dir_all(&dir).unwrap();
}
