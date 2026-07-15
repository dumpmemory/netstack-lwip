//! Exercises the shared-ownership lifetime binding: the process-global lwIP
//! stack must stay alive until the *last* of NetStack / TcpListener / UdpSocket
//! (and any SendHalf) is dropped, regardless of drop order, and a fresh
//! NetStack must be creatable only once the previous one is fully gone.
//!
//! Everything is in a single test so the global `NETSTACK_ALIVE` guard isn't
//! raced by parallel tests.

use netstack_lwip::NetStack;

#[test]
fn stack_lifecycle_and_drop_order() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(run());
}

async fn run() {
    let (stack, listener, udp) = NetStack::new().expect("create netstack");

    // Only one instance may exist at a time.
    assert!(
        NetStack::new().is_err(),
        "a second NetStack must fail while the first is alive"
    );

    // Dropping NetStack first must NOT tear down the shared stack: the listener
    // and udp socket still hold it, so re-creation stays blocked (and this must
    // not panic or deadlock).
    drop(stack);
    assert!(
        NetStack::new().is_err(),
        "stack still alive via listener/udp after NetStack dropped"
    );

    // A SendHalf alone must keep the stack alive after everything else is gone.
    let (send_half, recv_half) = udp.split();
    drop(listener);
    drop(recv_half);
    assert!(
        NetStack::new().is_err(),
        "stack still alive via a lone SendHalf"
    );

    // Dropping the final handle runs teardown synchronously (timer aborted,
    // callback cleared, guard released), so a fresh stack can now be built.
    drop(send_half);
    let (_s2, _l2, _u2) = NetStack::new().expect("recreate after full teardown");
}
