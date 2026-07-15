use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;

use futures::task::AtomicWaker;
use tokio::sync::mpsc::UnboundedSender;

/// State shared between a `TcpStream` and the lwIP C callbacks registered on its
/// pcb.
///
/// Every field is individually thread-safe, so this struct is `Sync` with no
/// `unsafe` and no surrounding lock: the callbacks reach it through a shared
/// `&TcpStreamContext` (from `tcp_arg`), never a `&mut`, so it can never alias
/// the `&mut TcpStream` held by the async side.
///
/// `LWIP_MUTEX` still serializes the lwIP calls themselves, but the correctness
/// of this struct no longer depends on that discipline.
pub struct TcpStreamContext {
    /// Peer address, kept only for logging.
    pub local_addr: SocketAddr,
    /// Carries received data — and an empty-vec sentinel on EOF/error — to
    /// `TcpStream::poll_read`. `send` needs only `&self`.
    pub read_tx: UnboundedSender<Vec<u8>>,
    /// Set by the lwIP error callback; once true the pcb is no longer valid.
    pub errored: AtomicBool,
    /// Registered by `poll_write`, woken by the sent/poll/err callbacks.
    pub write_waker: AtomicWaker,
}

impl TcpStreamContext {
    pub fn new(local_addr: SocketAddr, read_tx: UnboundedSender<Vec<u8>>) -> Self {
        TcpStreamContext {
            local_addr,
            read_tx,
            errored: AtomicBool::new(false),
            write_waker: AtomicWaker::new(),
        }
    }
}
