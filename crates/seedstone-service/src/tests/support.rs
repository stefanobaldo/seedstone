//! What more than one subject file needs: a served connection over a
//! `duplex` pipe, and the small readers and writers around it.

use crate::auth::Secret;
use crate::connection::serve_connection;
use crate::node::NodeInfo;
use seedstone_core::dict::DictSeed;
use seedstone_core::shard::{NoTrace, ShardPool};
use seedstone_resp::{Frame, parse};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};

/// Counts how many times the connection flushes and records the largest
/// single write, delegating everything else.
///
/// The write size is what makes the accumulation bound observable from
/// outside: the reply buffer is the connection's own, but every byte that
/// reaches the peer passes through here, so the largest write is exactly
/// the most one drain ever held.
pub struct FlushCounting<S> {
    pub inner: S,
    pub flushes: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pub max_write: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl<S: AsyncRead + Unpin> AsyncRead for FlushCounting<S> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for FlushCounting<S> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        self.max_write
            .fetch_max(buf.len(), std::sync::atomic::Ordering::Relaxed);
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        self.flushes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

pub fn node_with_password(pw: &[u8]) -> NodeInfo {
    let mut node = NodeInfo::for_tests();
    node.password = Some(Secret::new(pw.to_vec()));
    node
}

pub fn connected(
    shards: u16,
) -> (
    tokio::io::ReadHalf<tokio::io::DuplexStream>,
    tokio::io::WriteHalf<tokio::io::DuplexStream>,
    ShardPool,
) {
    // Four executors, or one per shard where there are fewer than four:
    // a pool may not have more executors than shards, and a test that
    // wants a single shard wants it precisely to remove the parallelism
    // that would hide what it is asserting.
    let pool = ShardPool::spawn(shards, shards.min(4), DictSeed { k0: 1, k1: 2 }, NoTrace);
    let (client, server) = tokio::io::duplex(64 * 1024);
    tokio::spawn(serve_connection(
        server,
        pool.clone(),
        NodeInfo::for_tests(),
    ));
    let (r, w) = tokio::io::split(client);
    (r, w, pool)
}

pub fn req(parts: &[&str]) -> Frame {
    Frame::Array(
        parts
            .iter()
            .map(|p| Frame::Bulk(p.as_bytes().to_vec()))
            .collect(),
    )
}

pub async fn read_frames<R: AsyncRead + Unpin>(r: &mut R, n: usize) -> Vec<Frame> {
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
