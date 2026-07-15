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
