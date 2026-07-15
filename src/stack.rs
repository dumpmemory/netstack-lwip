use std::marker::PhantomPinned;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{io, os::raw, pin::Pin, sync::Once, time};

use futures::sink::Sink;
use futures::stream::Stream;
use futures::task::{Context, Poll};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::JoinHandle;

use super::lwip::*;
use super::output::{output_ip4, output_ip6, OUTPUT_CB_PTR};
use super::tcp_listener::TcpListener;
use super::udp::UdpSocket;
use super::LWIP_MUTEX;
use crate::Error;

static LWIP_INIT: Once = Once::new();

// lwIP is a process-global singleton in this configuration (NO_SYS, one global
// netif, global pcb lists and `OUTPUT_CB_PTR`). Only one NetStack may exist at a
// time; this guards against a second one silently corrupting the first. Reset in
// `Drop`, so a new NetStack can be created after the previous one is dropped.
static NETSTACK_ALIVE: AtomicBool = AtomicBool::new(false);

pub struct NetStack {
    rx: Receiver<Vec<u8>>,
    sink_buf: Option<Vec<u8>>, // We're flushing per item, no need large buffer.
    // Raw pointer (kept as usize so `NetStack` stays `Send`) to a heap-allocated
    // `Sender` used only by the lwIP output callback. Owned by this `NetStack`
    // and freed on drop. Deliberately separate from `rx` so the callback never
    // has to form a reference to `NetStack`, which would alias the `&mut` taken
    // in `poll_next` and be undefined behaviour.
    output_tx: usize,
    // Background task driving lwIP's timers; aborted on drop so it doesn't
    // keep running (and calling into lwIP) after the stack is gone.
    timer: JoinHandle<()>,
    _pin: PhantomPinned,
}

impl NetStack {
    pub fn new() -> Result<(Pin<Box<Self>>, Pin<Box<TcpListener>>, Pin<Box<UdpSocket>>), Error> {
        Ok((
            NetStack::_new(512)?,
            TcpListener::new()?,
            UdpSocket::new(64)?,
        ))
    }

    pub fn with_buffer_size(
        stack_buffer_size: usize,
        udp_buffer_size: usize,
    ) -> Result<(Pin<Box<Self>>, Pin<Box<TcpListener>>, Pin<Box<UdpSocket>>), Error> {
        Ok((
            NetStack::_new(stack_buffer_size)?,
            TcpListener::new()?,
            UdpSocket::new(udp_buffer_size)?,
        ))
    }

    fn _new(buffer_size: usize) -> Result<Pin<Box<Self>>, Error> {
        if NETSTACK_ALIVE.swap(true, Ordering::AcqRel) {
            return Err(Error::AlreadyRunning);
        }

        LWIP_INIT.call_once(|| unsafe { lwip_init() });

        unsafe {
            (*netif_list).output = Some(output_ip4);
            (*netif_list).output_ip6 = Some(output_ip6);
            (*netif_list).mtu = 1500;
        }

        let (tx, rx): (Sender<Vec<u8>>, Receiver<Vec<u8>>) = channel(buffer_size);
        let output_tx = Box::into_raw(Box::new(tx)) as usize;

        let timer = tokio::spawn(async move {
            loop {
                {
                    let _g = LWIP_MUTEX.lock();
                    unsafe { sys_check_timeouts() };
                }
                tokio::time::sleep(time::Duration::from_millis(250)).await;
            }
        });

        let stack = Box::pin(NetStack {
            rx,
            sink_buf: None,
            output_tx,
            timer,
            _pin: PhantomPinned::default(),
        });

        unsafe {
            let _g = LWIP_MUTEX.lock();
            OUTPUT_CB_PTR = output_tx;
        }

        Ok(stack)
    }

}

impl Drop for NetStack {
    fn drop(&mut self) {
        log::trace!("drop netstack");
        // Stop the timer task so it no longer drives lwIP after the stack is
        // gone. It only holds LWIP_MUTEX inside a synchronous block (no `.await`
        // in scope), so aborting can never leave the lock held.
        self.timer.abort();
        unsafe {
            let _g = LWIP_MUTEX.lock();
            // Only clear the global if it still points at our Sender: a later
            // NetStack may have overwritten it.
            if OUTPUT_CB_PTR == self.output_tx {
                OUTPUT_CB_PTR = 0x0;
            }
            // Reclaim the callback Sender. Safe under the lock: the output
            // callback only runs while LWIP_MUTEX is held, so it cannot be
            // reading this pointer concurrently, and it will no longer see it.
            drop(Box::from_raw(self.output_tx as *mut Sender<Vec<u8>>));
        };
        // Allow a new NetStack to be created now that this one is gone.
        NETSTACK_ALIVE.store(false, Ordering::Release);
    }
}

impl Stream for NetStack {
    type Item = io::Result<Vec<u8>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = unsafe { self.get_unchecked_mut() };
        // `poll_recv` registers `cx`'s waker on `Pending` and the channel wakes
        // it when the output callback sends, so no manual waker is needed.
        match me.rx.poll_recv(cx) {
            Poll::Ready(Some(pkt)) => Poll::Ready(Some(Ok(pkt))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Sink<Vec<u8>> for NetStack {
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let me = unsafe { self.get_unchecked_mut() };
        if me.sink_buf.is_none() {
            Poll::Ready(Ok(()))
        } else {
            unsafe { Pin::new_unchecked(me) }.poll_flush(cx)
        }
    }

    fn start_send(self: Pin<&mut Self>, item: Vec<u8>) -> Result<(), Self::Error> {
        unsafe { self.get_unchecked_mut() }.sink_buf.replace(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let me = unsafe { self.get_unchecked_mut() };
        let item = match me.sink_buf.take() {
            Some(item) => item,
            None => return Poll::Ready(Ok(())),
        };
        if item.is_empty() {
            return Poll::Ready(Ok(()));
        }
        unsafe {
            let _g = LWIP_MUTEX.lock();

            let pbuf = pbuf_alloc(pbuf_layer_PBUF_RAW, item.len() as u16_t, pbuf_type_PBUF_RAM);
            if pbuf.is_null() {
                // lwIP is out of memory. Keep the packet (don't drop it) and
                // retry on a later poll. lwIP gives no "memory freed"
                // notification, so wake ourselves to reschedule rather than
                // stalling forever; memory is reclaimed as the timer task
                // processes ACKs/timeouts.
                log::trace!("pbuf_alloc null alloc");
                me.sink_buf = Some(item);
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            pbuf_take(
                pbuf,
                item.as_ptr() as *const raw::c_void,
                item.len() as u16_t,
            );

            if let Some(input_fn) = (*netif_list).input {
                let err = input_fn(pbuf, netif_list);
                if err == err_enum_t_ERR_OK as err_t {
                    Poll::Ready(Ok(()))
                } else {
                    pbuf_free(pbuf);
                    Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        format!("input error: {}", err),
                    )))
                }
            } else {
                pbuf_free(pbuf);
                Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "input fn not set",
                )))
            }
        }
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}
