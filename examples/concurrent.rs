//! Concurrent multi-connection, multi-protocol throughput/stress test for
//! `netstack-lwip`.
//!
//! Runs a matrix of flows **simultaneously** through a single `NetStack`:
//!   * TCP download  (netstack writes -> peer reads)
//!   * TCP upload    (peer writes    -> netstack reads)
//!   * UDP upload    (peer sends     -> netstack `recv_from`)
//!   * UDP download  (netstack `send_to` -> peer receives)
//!
//! As in `throughput.rs`, the peer is an in-process smoltcp stack wired to the
//! `NetStack` Sink/Stream (bare IP, no TUN, no root). All flows share one
//! smoltcp interface / socket set and one pair of pump tasks; the driver polls
//! every socket to quiescence each round.
//!
//! Direction is encoded in the destination port so the netstack side can route
//! each accepted connection / datagram to the right handler:
//!   * TCP download dials dst 10000+i, TCP upload dials 20000+i
//!   * UDP upload sends to dst 7000+i, UDP download targets peer port 18000+i
//!
//! Usage:
//!   cargo run --release --example concurrent -- \
//!       [--tcp-down N] [--tcp-up N] [--udp-up N] [--udp-down N] \
//!       [--bytes B] [--udp-bytes B] [--udp-dgram S]
//!
//! Defaults: tcp-down 4, tcp-up 4, udp-up 2, udp-down 2, bytes 32 MiB,
//! udp-bytes 16 MiB, udp-dgram 1400.

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::Notify;

use smoltcp::iface::{Config, Interface, PollResult, SocketHandle, SocketSet};
use smoltcp::phy::{Device, DeviceCapabilities, Medium, RxToken, TxToken};
use smoltcp::socket::{tcp, udp};
use smoltcp::time::Instant as SmolInstant;
use smoltcp::wire::{HardwareAddress, IpAddress, IpCidr, IpEndpoint};

use netstack_lwip::{NetStack, TcpStream};

const PEER_IP: IpAddress = IpAddress::v4(10, 0, 0, 2);
const NS_IP: IpAddress = IpAddress::v4(10, 0, 0, 1);

#[derive(Clone, Copy, PartialEq, Eq)]
enum Dir {
    Down, // TCP: netstack writes / peer reads.   UDP: netstack send_to / peer recv.
    Up,   // TCP: peer writes / netstack reads.    UDP: peer sends / netstack recv_from.
}

// Aggregate byte counters, updated by whichever side receives (or, for the
// tx counters, sends) so throughput and UDP loss can be computed at the end.
#[derive(Default)]
struct Stats {
    tcp_down_rx: AtomicU64,
    tcp_up_rx: AtomicU64,
    udp_up_tx: AtomicU64,
    udp_up_rx: AtomicU64,
    udp_down_tx: AtomicU64,
    udp_down_rx: AtomicU64,
}

// ---------------------------------------------------------------------------
// smoltcp in-memory device (same as throughput.rs): RX fed by lwIP output, TX
// drained into lwIP input, via tokio channels.
// ---------------------------------------------------------------------------

struct ChanDevice {
    rx: UnboundedReceiver<Vec<u8>>,
    tx: UnboundedSender<Vec<u8>>,
    mtu: usize,
}
struct RxTok(Vec<u8>);
struct TxTok(UnboundedSender<Vec<u8>>);

impl RxToken for RxTok {
    fn consume<R, F: FnOnce(&[u8]) -> R>(self, f: F) -> R {
        f(&self.0)
    }
}
impl TxToken for TxTok {
    fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, len: usize, f: F) -> R {
        let mut buf = vec![0u8; len];
        let r = f(&mut buf);
        let _ = self.0.send(buf);
        r
    }
}
impl Device for ChanDevice {
    type RxToken<'a> = RxTok;
    type TxToken<'a> = TxTok;
    fn receive(&mut self, _t: SmolInstant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        let pkt = self.rx.try_recv().ok()?;
        Some((RxTok(pkt), TxTok(self.tx.clone())))
    }
    fn transmit(&mut self, _t: SmolInstant) -> Option<Self::TxToken<'_>> {
        Some(TxTok(self.tx.clone()))
    }
    fn capabilities(&self) -> DeviceCapabilities {
        let mut caps = DeviceCapabilities::default();
        caps.medium = Medium::Ip;
        caps.max_transmission_unit = self.mtu;
        caps
    }
}

struct TcpFlow {
    h: SocketHandle,
    dir: Dir,
    target: u64,
    sent: u64,
    recv: u64,
    closing: bool,
    done: bool,
}
struct UdpFlow {
    h: SocketHandle,
    dir: Dir,
    target: u64,
    sent: u64,
    recv: u64,
    remote: IpEndpoint,
    done: bool,
}

struct Config_ {
    tcp_down: usize,
    tcp_up: usize,
    udp_up: usize,
    udp_down: usize,
    tcp_bytes: u64,
    udp_bytes: u64,
    dgram: usize,
}

fn main() {
    let cfg = parse_args();
    let n_tcp = cfg.tcp_down + cfg.tcp_up;
    // Every flow has exactly one sender that bumps `send_complete` on finishing
    // its target: TCP-down (netstack writer), TCP-up (peer/driver), UDP-up
    // (peer/driver), UDP-down (netstack sender). Teardown waits for all of them.
    let n_senders = cfg.tcp_down + cfg.tcp_up + cfg.udp_up + cfg.udp_down;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(run(cfg, n_tcp, n_senders));

    println!("\ndone.");
}

async fn run(cfg: Config_, n_tcp: usize, n_senders: usize) {
    println!(
        "concurrent: TCP {}+{} (down+up) x {} MiB, UDP {}+{} (up+down) x {} MiB, {}-byte datagrams\n",
        cfg.tcp_down,
        cfg.tcp_up,
        cfg.tcp_bytes / (1024 * 1024),
        cfg.udp_up,
        cfg.udp_down,
        cfg.udp_bytes / (1024 * 1024),
        cfg.dgram,
    );

    // Bigger buffers than the default: many flows multiplex onto one stack, so
    // a small shared output channel would drop packets (harmless retransmits
    // for TCP, but extra loss for UDP).
    let (stack, mut listener, udp) =
        NetStack::with_buffer_size(4096, 1024).expect("create netstack");
    let (mut sink, mut stream) = stack.split();
    let (udp_send, mut udp_recv) = udp.split();
    let udp_send = Arc::new(udp_send);

    let (in_tx, in_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let wake = Arc::new(Notify::new());

    let stats = Arc::new(Stats::default());
    let stop = Arc::new(AtomicBool::new(false));
    let tcp_complete = Arc::new(AtomicUsize::new(0)); // TCP receivers that got all bytes
    let send_complete = Arc::new(AtomicUsize::new(0)); // senders that sent all bytes

    // ---- packet pumps ----
    let pump_in = {
        let wake = wake.clone();
        tokio::spawn(async move {
            while let Some(item) = stream.next().await {
                match item {
                    Ok(pkt) => {
                        if in_tx.send(pkt).is_err() {
                            break;
                        }
                        wake.notify_one();
                    }
                    Err(_) => break,
                }
            }
        })
    };
    let pump_out = tokio::spawn(async move {
        while let Some(pkt) = out_rx.recv().await {
            if sink.send(pkt).await.is_err() {
                break;
            }
        }
    });

    // ---- smoltcp driver: the peer side of every flow ----
    let driver = {
        let stats = stats.clone();
        let stop = stop.clone();
        let tcp_complete = tcp_complete.clone();
        let send_complete = send_complete.clone();
        let wake = wake.clone();
        tokio::spawn(async move {
            let mut dev = ChanDevice { rx: in_rx, tx: out_tx, mtu: 1500 };
            let mut iface = Interface::new(
                Config::new(HardwareAddress::Ip),
                &mut dev,
                SmolInstant::now(),
            );
            iface.update_ip_addrs(|a| a.push(IpCidr::new(PEER_IP, 24)).unwrap());

            let mut sockets = SocketSet::new(Vec::new());
            let mut tcp_flows: Vec<TcpFlow> = Vec::new();
            let mut udp_flows: Vec<UdpFlow> = Vec::new();

            // TCP download: peer connects to ns dst 10000+i, then reads.
            for i in 0..cfg.tcp_down {
                let s = new_tcp_socket();
                let h = sockets.add(s);
                sockets
                    .get_mut::<tcp::Socket>(h)
                    .connect(iface.context(), (NS_IP, 10000 + i as u16), 40000 + i as u16)
                    .expect("connect tcp-down");
                tcp_flows.push(TcpFlow { h, dir: Dir::Down, target: cfg.tcp_bytes, sent: 0, recv: 0, closing: false, done: false });
            }
            // TCP upload: peer connects to ns dst 20000+i, then writes.
            for i in 0..cfg.tcp_up {
                let s = new_tcp_socket();
                let h = sockets.add(s);
                sockets
                    .get_mut::<tcp::Socket>(h)
                    .connect(iface.context(), (NS_IP, 20000 + i as u16), 45000 + i as u16)
                    .expect("connect tcp-up");
                tcp_flows.push(TcpFlow { h, dir: Dir::Up, target: cfg.tcp_bytes, sent: 0, recv: 0, closing: false, done: false });
            }
            // UDP upload: peer sends to ns dst 7000+i.
            for i in 0..cfg.udp_up {
                let s = new_udp_socket();
                let h = sockets.add(s);
                sockets.get_mut::<udp::Socket>(h).bind(17000 + i as u16).unwrap();
                udp_flows.push(UdpFlow { h, dir: Dir::Up, target: cfg.udp_bytes, sent: 0, recv: 0, remote: IpEndpoint::new(NS_IP, 7000 + i as u16), done: false });
            }
            // UDP download: peer receives on port 18000+i (netstack sends there).
            for i in 0..cfg.udp_down {
                let s = new_udp_socket();
                let h = sockets.add(s);
                sockets.get_mut::<udp::Socket>(h).bind(18000 + i as u16).unwrap();
                udp_flows.push(UdpFlow { h, dir: Dir::Down, target: cfg.udp_bytes, sent: 0, recv: 0, remote: IpEndpoint::new(NS_IP, 0), done: false });
            }

            let tcp_send_buf = vec![0x5Au8; 64 * 1024];
            let udp_send_buf = vec![0x5Au8; cfg.dgram];
            let mut scratch = vec![0u8; 64 * 1024];

            while !stop.load(Ordering::Relaxed) {
                // Drive to quiescence: service every socket and poll until
                // nothing more happens, so a full window goes out per flow.
                loop {
                    for f in &mut tcp_flows {
                        let s = sockets.get_mut::<tcp::Socket>(f.h);
                        match f.dir {
                            Dir::Down => {
                                while s.can_recv() {
                                    match s.recv_slice(&mut scratch) {
                                        Ok(0) | Err(_) => break,
                                        Ok(n) => {
                                            f.recv += n as u64;
                                            stats.tcp_down_rx.fetch_add(n as u64, Ordering::Relaxed);
                                            if f.recv >= f.target && !f.done {
                                                f.done = true;
                                                tcp_complete.fetch_add(1, Ordering::Relaxed);
                                            }
                                        }
                                    }
                                }
                            }
                            Dir::Up => {
                                while f.sent < f.target && s.can_send() {
                                    let want = ((f.target - f.sent) as usize).min(tcp_send_buf.len());
                                    match s.send_slice(&tcp_send_buf[..want]) {
                                        Ok(0) | Err(_) => break,
                                        Ok(n) => f.sent += n as u64,
                                    }
                                }
                                if f.sent >= f.target && !f.closing {
                                    s.close();
                                    f.closing = true;
                                    send_complete.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                    for f in &mut udp_flows {
                        let s = sockets.get_mut::<udp::Socket>(f.h);
                        match f.dir {
                            Dir::Up => {
                                while f.sent < f.target && s.can_send() {
                                    let n = ((f.target - f.sent) as usize).min(cfg.dgram);
                                    match s.send_slice(&udp_send_buf[..n], f.remote) {
                                        Ok(()) => {
                                            f.sent += n as u64;
                                            stats.udp_up_tx.fetch_add(n as u64, Ordering::Relaxed);
                                        }
                                        Err(_) => break, // tx buffer full; retry next pass
                                    }
                                }
                                if f.sent >= f.target && !f.done {
                                    f.done = true;
                                    send_complete.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            Dir::Down => {
                                while s.can_recv() {
                                    match s.recv_slice(&mut scratch) {
                                        Ok((0, _)) | Err(_) => break,
                                        Ok((n, _)) => {
                                            f.recv += n as u64;
                                            stats.udp_down_rx.fetch_add(n as u64, Ordering::Relaxed);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if iface.poll(SmolInstant::now(), &mut dev, &mut sockets) == PollResult::None {
                        break;
                    }
                }

                let _ = tokio::time::timeout(Duration::from_millis(1), wake.notified()).await;
            }
        })
    };

    // ---- netstack side: accept TCP connections and route by dst port ----
    let accept = {
        let stats = stats.clone();
        let stop = stop.clone();
        let tcp_complete = tcp_complete.clone();
        let send_complete = send_complete.clone();
        let tcp_down = cfg.tcp_down;
        let tcp_bytes = cfg.tcp_bytes;
        tokio::spawn(async move {
            let mut handles = Vec::new();
            for _ in 0..n_tcp {
                let (ns, _local, remote) = listener.next().await.expect("accept");
                // remote_addr() is the destination the peer dialed.
                let dir = if (remote.port() as usize) < 10000 + tcp_down {
                    Dir::Down // dst 10000+i -> netstack writes
                } else {
                    Dir::Up // dst 20000+i -> netstack reads
                };
                handles.push(tokio::spawn(handle_tcp(
                    ns,
                    dir,
                    tcp_bytes,
                    stats.clone(),
                    tcp_complete.clone(),
                    send_complete.clone(),
                    stop.clone(),
                )));
            }
            for h in handles {
                let _ = h.await;
            }
        })
    };

    // ---- netstack side: receive all inbound UDP (the udp-up flows) ----
    let udp_rx_task = {
        let stats = stats.clone();
        tokio::spawn(async move {
            while let Ok((payload, _src, _dst)) = udp_recv.recv_from().await {
                stats.udp_up_rx.fetch_add(payload.len() as u64, Ordering::Relaxed);
            }
        })
    };

    // ---- netstack side: send the udp-down flows ----
    let mut udp_tx_tasks = Vec::new();
    for i in 0..cfg.udp_down {
        let stats = stats.clone();
        let stop = stop.clone();
        let send_complete = send_complete.clone();
        let udp_send = udp_send.clone();
        let target = cfg.udp_bytes;
        let dgram = cfg.dgram;
        udp_tx_tasks.push(tokio::spawn(async move {
            let src = format!("10.0.0.1:{}", 8000 + i as u16).parse().unwrap();
            let dst = format!("10.0.0.2:{}", 18000 + i as u16).parse().unwrap();
            let data = vec![0x5Au8; dgram];
            let mut sent: u64 = 0;
            let mut since_yield = 0u32;
            while sent < target {
                let n = ((target - sent) as usize).min(dgram);
                if udp_send.send_to(&data[..n], &src, &dst).is_ok() {
                    sent += n as u64;
                    stats.udp_down_tx.fetch_add(n as u64, Ordering::Relaxed);
                }
                // Don't monopolise the worker: yield periodically so the pumps
                // and receivers make progress (and drops reflect real capacity).
                since_yield += 1;
                if since_yield >= 32 {
                    since_yield = 0;
                    tokio::task::yield_now().await;
                }
            }
            send_complete.fetch_add(1, Ordering::Relaxed);
            while !stop.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }));
    }

    // ---- coordinate: wait for completion, then a grace period for UDP drain ----
    let t0 = Instant::now();
    while tcp_complete.load(Ordering::Relaxed) < n_tcp {
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let t_tcp = t0.elapsed();
    while send_complete.load(Ordering::Relaxed) < n_senders {
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let t_send = t0.elapsed();
    // Let lossy UDP drain through the pipe before we sample its counters.
    tokio::time::sleep(Duration::from_millis(300)).await;

    report(&cfg, &stats, t_tcp, t_send);

    // Tear everything down so the singleton is released.
    stop.store(true, Ordering::Relaxed);
    wake.notify_one();
    accept.abort();
    udp_rx_task.abort();
    for t in udp_tx_tasks {
        t.abort();
    }
    driver.abort();
    pump_in.abort();
    pump_out.abort();
    let _ = accept.await;
    let _ = driver.await;
    let _ = pump_in.await;
    let _ = pump_out.await;
}

async fn handle_tcp(
    mut ns: Pin<Box<TcpStream>>,
    dir: Dir,
    target: u64,
    stats: Arc<Stats>,
    tcp_complete: Arc<AtomicUsize>,
    send_complete: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
) {
    match dir {
        Dir::Down => {
            // netstack writes; the peer (driver) counts and completes.
            let buf = vec![0xA5u8; 64 * 1024];
            let mut sent: u64 = 0;
            while sent < target {
                let n = ((target - sent) as usize).min(buf.len());
                if ns.write_all(&buf[..n]).await.is_err() {
                    break;
                }
                sent += n as u64;
            }
            ns.flush().await.ok();
            ns.shutdown().await.ok();
            send_complete.fetch_add(1, Ordering::Relaxed);
        }
        Dir::Up => {
            // netstack reads; it is the receiver, so it completes.
            let mut buf = vec![0u8; 64 * 1024];
            let mut recv: u64 = 0;
            loop {
                match ns.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        recv += n as u64;
                        stats.tcp_up_rx.fetch_add(n as u64, Ordering::Relaxed);
                        if recv >= target {
                            tcp_complete.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                    }
                }
            }
        }
    }
    // Hold the connection open until teardown so a premature drop can't reset
    // it before the peer has drained the last bytes.
    while !stop.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn new_tcp_socket() -> tcp::Socket<'static> {
    let rx = tcp::SocketBuffer::new(vec![0u8; 1 << 20]);
    let tx = tcp::SocketBuffer::new(vec![0u8; 1 << 20]);
    let mut s = tcp::Socket::new(rx, tx);
    s.set_nagle_enabled(false);
    s.set_ack_delay(None);
    s
}

fn new_udp_socket() -> udp::Socket<'static> {
    let rx = udp::PacketBuffer::new(
        vec![udp::PacketMetadata::EMPTY; 256],
        vec![0u8; 1 << 20],
    );
    let tx = udp::PacketBuffer::new(
        vec![udp::PacketMetadata::EMPTY; 256],
        vec![0u8; 1 << 20],
    );
    udp::Socket::new(rx, tx)
}

fn gbps(bytes: u64, secs: f64) -> f64 {
    bytes as f64 * 8.0 / 1e9 / secs.max(f64::MIN_POSITIVE)
}

fn report(cfg: &Config_, stats: &Stats, t_tcp: Duration, t_send: Duration) {
    let tcp_secs = t_tcp.as_secs_f64();
    let udp_secs = t_send.as_secs_f64();
    let tcp_down = stats.tcp_down_rx.load(Ordering::Relaxed);
    let tcp_up = stats.tcp_up_rx.load(Ordering::Relaxed);
    let uut = stats.udp_up_tx.load(Ordering::Relaxed);
    let uur = stats.udp_up_rx.load(Ordering::Relaxed);
    let udt = stats.udp_down_tx.load(Ordering::Relaxed);
    let udr = stats.udp_down_rx.load(Ordering::Relaxed);

    let loss = |tx: u64, rx: u64| if tx == 0 { 0.0 } else { (1.0 - rx as f64 / tx as f64) * 100.0 };

    println!("--- results ---");
    println!(
        "TCP total: {} flows, {:.1} Gbps aggregate in {:.3}s",
        cfg.tcp_down + cfg.tcp_up,
        gbps(tcp_down + tcp_up, tcp_secs),
        tcp_secs,
    );
    if cfg.tcp_down > 0 {
        println!(
            "  TCP down: {} flows -> {:.1} Gbps ({} MiB)",
            cfg.tcp_down, gbps(tcp_down, tcp_secs), tcp_down / (1024 * 1024)
        );
    }
    if cfg.tcp_up > 0 {
        println!(
            "  TCP up:   {} flows -> {:.1} Gbps ({} MiB)",
            cfg.tcp_up, gbps(tcp_up, tcp_secs), tcp_up / (1024 * 1024)
        );
    }
    println!(
        "UDP total: {} flows, senders finished in {:.3}s",
        cfg.udp_up + cfg.udp_down,
        udp_secs,
    );
    if cfg.udp_up > 0 {
        println!(
            "  UDP up:   {} flows -> sent {} MiB, recv {} MiB, {:.1} Gbps goodput, {:.1}% loss",
            cfg.udp_up, uut / (1024 * 1024), uur / (1024 * 1024), gbps(uur, udp_secs), loss(uut, uur)
        );
    }
    if cfg.udp_down > 0 {
        println!(
            "  UDP down: {} flows -> sent {} MiB, recv {} MiB, {:.1} Gbps goodput, {:.1}% loss",
            cfg.udp_down, udt / (1024 * 1024), udr / (1024 * 1024), gbps(udr, udp_secs), loss(udt, udr)
        );
    }
}

fn parse_args() -> Config_ {
    let mut c = Config_ {
        tcp_down: 4,
        tcp_up: 4,
        udp_up: 2,
        udp_down: 2,
        tcp_bytes: 32 * 1024 * 1024,
        udp_bytes: 16 * 1024 * 1024,
        dgram: 1400,
    };
    let mut args = std::env::args().skip(1);
    macro_rules! val {
        () => {
            args.next().and_then(|v| v.parse().ok()).expect("value")
        };
    }
    while let Some(a) = args.next() {
        match a.as_str() {
            "--tcp-down" => c.tcp_down = val!(),
            "--tcp-up" => c.tcp_up = val!(),
            "--udp-up" => c.udp_up = val!(),
            "--udp-down" => c.udp_down = val!(),
            "--bytes" => c.tcp_bytes = val!(),
            "--udp-bytes" => c.udp_bytes = val!(),
            "--udp-dgram" => c.dgram = val!(),
            "-h" | "--help" => {
                eprintln!("usage: concurrent [--tcp-down N] [--tcp-up N] [--udp-up N] [--udp-down N] [--bytes B] [--udp-bytes B] [--udp-dgram S]");
                std::process::exit(0);
            }
            other => panic!("unknown arg {other:?}"),
        }
    }
    c
}
