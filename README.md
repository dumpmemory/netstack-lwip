A netstack for the special purpose of turning packets from/to a TUN interface into TCP streams and UDP packets. It uses lwIP as the backend netstack.

```rust
let (stack, mut tcp_listener, udp_socket) = netstack::NetStack::new();
let (mut stack_sink, mut stack_stream) = stack.split();
let (mut tun_sink, mut tun_stream) = tun.split(); // tun is assumed implementing `Stream` and `Sink`

// Reads packet from stack and sends to TUN.
tokio::spawn(async move {
    while let Some(pkt) = stack_stream.next().await {
        if let Ok(pkt) = pkt {
            tun_sink.send(pkt).await.unwrap();
        }
    }
});

// Reads packet from TUN and sends to stack.
tokio::spawn(async move {
    while let Some(pkt) = tun_stream.next().await {
        if let Ok(pkt) = pkt {
            stack_sink.send(pkt).await.unwrap();
        }
    }
});

// Extracts TCP connections from stack and sends them to the dispatcher.
tokio::spawn(async move {
    while let Some((stream, local_addr, remote_addr)) = tcp_listener.next().await {
        tokio::spawn(handle_inbound_stream(
            stream,
            local_addr,
            remote_addr,
        ));
    }
});

// Receive and send UDP packets between netstack and NAT manager. The NAT
// manager would maintain UDP sessions and send them to the dispatcher.
tokio::spawn(async move {
    handle_inbound_datagram(udp_socket).await;
});
```

## Testing

Run the unit and integration tests:

```sh
cargo test
```

This includes `tests/lifecycle.rs`, which exercises the shared-ownership
lifetime binding (the process-global lwIP stack must stay alive until the last
handle is dropped, and must be re-creatable afterwards).

## Throughput benchmark

`examples/throughput.rs` measures end-to-end TCP throughput in both directions.
Because `NetStack` only *accepts* connections (it never dials), the benchmark
uses [smoltcp](https://github.com/smoltcp-rs/smoltcp) as an in-process,
userspace TCP peer wired directly to the `NetStack` `Sink`/`Stream` — so it
needs no TUN device and no root, and runs anywhere:

```sh
# Both directions, 256 MiB each (defaults). Release build strongly recommended.
cargo run --release --example throughput

# Options:
#   --bytes N   bytes to transfer per direction (default 268435456 = 256 MiB)
#   --dir D     up | down | both (default both)
#   --chunk N   application write size in bytes (default 65536 = 64 KiB)
cargo run --release --example throughput -- --bytes 67108864 --dir both --chunk 65536
```

It exercises the full data path (download: `TcpStream` write → `tcp_output` →
stack `Stream`; upload: stack `Sink` → `tcp_in` → `TcpStream` read), asserts the
exact byte count transferred, and prints MiB/s and Gbps per direction.

The smoltcp peer is polled to quiescence each round so a full TCP window is kept
in flight (smoltcp emits at most one segment per `poll()`); polling once per
round would collapse the transfer to one segment per round-trip. Because the
pipe is in-memory, throughput is dominated by per-packet CPU cost, and small
transfers are dominated by TCP slow-start — use a large `--bytes` (≥256 MiB) for
a representative steady-state number.

## Concurrent multi-connection / multi-protocol test

`examples/concurrent.rs` drives many flows through a single `NetStack`
**simultaneously** — TCP download + TCP upload + UDP upload + UDP download — and
reports per-group aggregate throughput plus UDP loss:

```sh
# Defaults: 4 TCP down, 4 TCP up, 2 UDP up, 2 UDP down.
cargo run --release --example concurrent

# Options: --tcp-down N --tcp-up N --udp-up N --udp-down N
#          --bytes B (per TCP flow) --udp-bytes B (per UDP flow) --udp-dgram S
cargo run --release --example concurrent -- --tcp-down 16 --tcp-up 16 --udp-up 4 --udp-down 4
```

All flows share one lwIP stack, so aggregate throughput is bounded by the single
global lock serialising every call into lwIP; expect aggregate throughput to be
lower than a single stream and to fall as the flow count rises. UDP has no flow
control, so datagrams may be dropped (the test reports the loss rate).

## Connection capacity & memory

The stack is a VPN-style netstack meant to carry every connection on the device.
Simultaneous TCP connection capacity is set by `MEMP_NUM_TCP_PCB` in
`src/lwip/custom/lwipopts.h`, tuned per platform:

| Platform | Max TCP connections | `MEM_SIZE` (heap) | `TCP_WND` / `TCP_SND_BUF` |
| --- | --- | --- | --- |
| iOS | ~512 | 1 MiB | 16·MSS / 8·MSS |
| macOS / Linux / other | ~2048 | 8 MiB | 32·MSS / 16·MSS |

How the limits work:

- Each simultaneous connection needs one `tcp_pcb` (256 B). Idle/established
  connections cost only that; `MEMP_NUM_TCP_PCB` is the hard ceiling. There is no
  listen-backlog cap (`TCP_LISTEN_BACKLOG` is off).
- Closed connections linger in `TIME_WAIT` for `2 * TCP_MSL` (here `TCP_MSL` is
  10 s → 20 s), each holding a `tcp_pcb` from the same pool. lwIP reclaims the
  oldest `TIME_WAIT` (then `LAST_ACK`/`CLOSING`) before ever evicting an active
  connection, so connection churn never lowers active capacity. `TCP_MSL` is
  shortened from lwIP's 60 s default because these connections are device-local
  (no WAN path, so no stale duplicates to outlive), which keeps `TIME_WAIT` from
  piling up under the churn a VPN sees.
- Actively *transferring* connections additionally draw from the shared
  `MEM_SIZE` heap (send buffers) and `MEMP_NUM_TCP_SEG` segment pool. These are
  sized for realistic concurrent transfer, not for every connection at once;
  exhausting them throttles a sender gracefully (it does not drop connections).
- `MEM_SIZE` is a lazily-paged heap (only touched pages cost RAM); the `memp`
  pools are fully allocated at init. `PBUF_POOL` is unused on this data path
  (RX/TX use `PBUF_RAM`, UDP uses `PBUF_REF`), so its pool is kept minimal.
- iOS uses a smaller window because the lwIP TCP is device-local (app ↔ netstack,
  microsecond RTT) — a small window still saturates it, while bounding how much
  unread data can buffer per connection. Received data waits in an unbounded
  channel until read, up to roughly one `TCP_WND` per connection, so the
  application should drain accepted `TcpStream`s promptly.
