use std::marker::PhantomPinned;
use std::sync::atomic::Ordering;
use std::{cmp::min, io, net::SocketAddr, os::raw, pin::Pin};

use bytes::BytesMut;
use futures::task::{Context, Poll};
use log::*;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::mpsc::{unbounded_channel, UnboundedReceiver},
};

use super::lwip::*;
use super::tcp_stream_context::TcpStreamContext;
use super::util;
use super::LWIP_MUTEX;

#[allow(unused_variables)]
pub unsafe extern "C" fn tcp_recv_cb(
    arg: *mut raw::c_void,
    tpcb: *mut tcp_pcb,
    p: *mut pbuf,
    err: err_t,
) -> err_t {
    if arg.is_null() {
        warn!("tcp connection has been closed");
        return err_enum_t_ERR_CONN as err_t;
    }

    // SAFETY: `arg` points to the `TcpStreamContext` of a pinned, still-alive
    // TcpStream (cleared to null in its Drop). We form only a shared reference;
    // every field is thread-safe.
    let ctx = &*(arg as *const TcpStreamContext);

    if p.is_null() {
        trace!("netstack tcp eof {}", ctx.local_addr);
        let _ = ctx.read_tx.send(Vec::new());
        return err_enum_t_ERR_OK as err_t;
    }

    let pbuflen = std::ptr::read_unaligned(p).tot_len;
    let mut buf = Vec::with_capacity(pbuflen as usize);
    pbuf_copy_partial(p, buf.as_mut_ptr() as _, pbuflen, 0);
    buf.set_len(pbuflen as usize);

    if !buf.is_empty() {
        let _ = ctx.read_tx.send(buf);
    }

    pbuf_free(p);
    err_enum_t_ERR_OK as err_t
}

#[allow(unused_variables)]
pub extern "C" fn tcp_sent_cb(arg: *mut raw::c_void, tpcb: *mut tcp_pcb, len: u16_t) -> err_t {
    if arg.is_null() {
        return err_enum_t_ERR_OK as err_t;
    }
    // SAFETY: see `tcp_recv_cb`.
    let ctx = unsafe { &*(arg as *const TcpStreamContext) };
    ctx.write_waker.wake();
    err_enum_t_ERR_OK as err_t
}

#[allow(unused_variables)]
pub extern "C" fn tcp_err_cb(arg: *mut ::std::os::raw::c_void, err: err_t) {
    if arg.is_null() {
        return;
    }
    // SAFETY: see `tcp_recv_cb`.
    let ctx = unsafe { &*(arg as *const TcpStreamContext) };
    trace!("netstack tcp err {} {}", err, ctx.local_addr);
    ctx.errored.store(true, Ordering::Release);
    // Wake a parked reader via an empty-vec sentinel and a parked writer.
    let _ = ctx.read_tx.send(Vec::new());
    ctx.write_waker.wake();
}

#[allow(unused_variables)]
pub extern "C" fn tcp_poll_cb(arg: *mut ::std::os::raw::c_void, tpcb: *mut tcp_pcb) -> err_t {
    if arg.is_null() {
        return err_enum_t_ERR_OK as err_t;
    }
    // SAFETY: see `tcp_recv_cb`.
    let ctx = unsafe { &*(arg as *const TcpStreamContext) };
    ctx.write_waker.wake();
    err_enum_t_ERR_OK as err_t
}

pub struct TcpStream {
    src_addr: SocketAddr,
    dest_addr: SocketAddr,
    pcb: usize,
    // Overflow buffer for read data that didn't fit the caller's buffer.
    write_buf: BytesMut,
    callback_ctx: TcpStreamContext,
    // Receiving end of the channel fed by `tcp_recv_cb`; owned solely by the
    // async side, so it needs no sharing/locking.
    read_rx: UnboundedReceiver<Vec<u8>>,
    // Whether the write side has been shut down; touched only by the async side.
    closed: bool,
    is_eof: bool,
    _pin: PhantomPinned,
}

impl TcpStream {
    pub fn new(pcb: *mut tcp_pcb) -> Pin<Box<Self>> {
        unsafe {
            // Since we have no idea how to deal with a full bounded channel upon receiving
            // data from lwIP, an unbounded channel is used instead.
            //
            // Note that lwIP is in charge of flow control. If reader is slower than writer,
            // lwIP will propagate the pressure back by announcing a decreased window size.
            // Thus our unbounded channel will never be overwhelmed. To achieve this, we must
            // call `tcp_recved` when the data from our internal buffer are consumed.
            let (read_tx, read_rx) = unbounded_channel();
            let pcb_v = std::ptr::read_unaligned(pcb);
            let src_addr = util::to_socket_addr(&pcb_v.remote_ip, pcb_v.remote_port);
            let dest_addr = util::to_socket_addr(&pcb_v.local_ip, pcb_v.local_port);
            let stream = Box::pin(TcpStream {
                src_addr,
                dest_addr,
                pcb: pcb as usize,
                write_buf: BytesMut::new(),
                callback_ctx: TcpStreamContext::new(src_addr, read_tx),
                read_rx,
                closed: false,
                is_eof: false,
                _pin: PhantomPinned::default(),
            });
            let arg = &stream.callback_ctx as *const _;
            tcp_arg(pcb, arg as *mut raw::c_void);
            tcp_recv(pcb, Some(tcp_recv_cb));
            tcp_sent(pcb, Some(tcp_sent_cb));
            tcp_err(pcb, Some(tcp_err_cb));
            tcp_poll(pcb, Some(tcp_poll_cb), 8 as _);
            stream.apply_pcb_opts();
            trace!("netstack tcp new {}", stream.local_addr());
            stream
        }
    }

    fn apply_pcb_opts(&self) {
        unsafe {
            let mut pcb_v = std::ptr::read_unaligned(self.pcb as *const tcp_pcb);
            #[cfg(target_os = "ios")]
            {
                pcb_v.so_options |= SOF_KEEPALIVE as u8;
            }
            pcb_v.flags |= TF_NODELAY as u16;
            std::ptr::write_unaligned(self.pcb as *mut tcp_pcb, pcb_v);
        }
    }

    pub fn local_addr(&self) -> &SocketAddr {
        &self.src_addr
    }

    pub fn remote_addr(&self) -> &SocketAddr {
        &self.dest_addr
    }

    fn send_buf_size(&self) -> usize {
        unsafe { std::ptr::read_unaligned(self.pcb as *const tcp_pcb).snd_buf as usize }
    }
}

fn broken_pipe() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "broken pipe")
}

impl AsyncRead for TcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<io::Result<()>> {
        let me = unsafe { self.get_unchecked_mut() };
        let _guard = LWIP_MUTEX.lock();
        if me.callback_ctx.errored.load(Ordering::Acquire) {
            return Poll::Ready(Err(broken_pipe()));
        }
        if !me.write_buf.is_empty() {
            let to_read = min(buf.remaining(), me.write_buf.len());
            let piece = me.write_buf.split_to(to_read);
            buf.put_slice(&piece[..to_read]);
            return Poll::Ready(Ok(()));
        }
        let mut has_read_data = false;
        loop {
            match Pin::new(&mut me.read_rx).poll_recv(cx) {
                Poll::Ready(Some(data)) => {
                    // An empty vec is a sentinel: EOF, or a broken pipe if the
                    // error callback fired.
                    if data.is_empty() {
                        if me.callback_ctx.errored.load(Ordering::Acquire) {
                            return Poll::Ready(Err(broken_pipe()));
                        }
                        me.is_eof = true;
                        return Poll::Ready(Ok(()));
                    }
                    unsafe { tcp_recved(me.pcb as *mut tcp_pcb, data.len() as u16_t) };
                    let to_read = min(buf.remaining(), data.len());
                    buf.put_slice(&data[..to_read]);
                    has_read_data = true;
                    if to_read < data.len() {
                        me.write_buf.extend_from_slice(&data[to_read..]);
                        return Poll::Ready(Ok(()));
                    }
                }
                Poll::Ready(None) => return Poll::Ready(Err(broken_pipe())),
                Poll::Pending => {
                    return if has_read_data {
                        Poll::Ready(Ok(()))
                    } else if me.is_eof {
                        Poll::Ready(Ok(()))
                    } else {
                        Poll::Pending
                    };
                }
            }
        }
    }
}

impl Drop for TcpStream {
    fn drop(&mut self) {
        let _guard = LWIP_MUTEX.lock();
        trace!("netstack tcp drop {}", &self.callback_ctx.local_addr);
        if !self.callback_ctx.errored.load(Ordering::Acquire) {
            unsafe {
                tcp_arg(self.pcb as *mut tcp_pcb, std::ptr::null_mut());
                tcp_recv(self.pcb as *mut tcp_pcb, None);
                tcp_sent(self.pcb as *mut tcp_pcb, None);
                tcp_err(self.pcb as *mut tcp_pcb, None);
                tcp_poll(self.pcb as *mut tcp_pcb, None, 0);
                if !self.closed {
                    tcp_abort(self.pcb as *mut tcp_pcb);
                }
            }
        }
    }
}

impl AsyncWrite for TcpStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<io::Result<usize>> {
        let _guard = LWIP_MUTEX.lock();
        if self.callback_ctx.errored.load(Ordering::Acquire) {
            return Poll::Ready(Err(broken_pipe()));
        }
        let to_write = buf.len().min(self.send_buf_size());
        if to_write == 0 {
            self.callback_ctx.write_waker.register(cx.waker());
            return Poll::Pending;
        }
        let err = unsafe {
            tcp_write(
                self.pcb as *mut tcp_pcb,
                buf.as_ptr() as *const raw::c_void,
                to_write as u16_t,
                TCP_WRITE_FLAG_COPY as u8,
            )
        };
        if err == err_enum_t_ERR_OK as err_t {
            // Call output in case of mem err?
            let err = unsafe { tcp_output(self.pcb as *mut tcp_pcb) };
            if err == err_enum_t_ERR_OK as err_t {
                Poll::Ready(Ok(to_write))
            } else {
                Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    format!("netstack tcp_output error {}", err),
                )))
            }
        } else if err == err_enum_t_ERR_MEM as err_t {
            // trace!("netstack tcp err_mem on {}", &local_addr);
            self.callback_ctx.write_waker.register(cx.waker());
            Poll::Pending
        } else {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Interrupted,
                format!("netstack tcp_write error {}", err),
            )))
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<io::Result<()>> {
        let _guard = LWIP_MUTEX.lock();
        if self.callback_ctx.errored.load(Ordering::Acquire) {
            return Poll::Ready(Err(broken_pipe()));
        }
        let err = unsafe { tcp_output(self.pcb as *mut tcp_pcb) };
        if err != err_enum_t_ERR_OK as err_t {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Interrupted,
                format!("netstack tcp_output error {}", err),
            )))
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<io::Result<()>> {
        let me = unsafe { self.get_unchecked_mut() };
        let _guard = LWIP_MUTEX.lock();
        if me.callback_ctx.errored.load(Ordering::Acquire) {
            return Poll::Ready(Err(broken_pipe()));
        }
        trace!("netstack tcp shutdown {}", &me.callback_ctx.local_addr);
        let err = unsafe { tcp_shutdown(me.pcb as *mut tcp_pcb, 0, 1) };
        if err != err_enum_t_ERR_OK as err_t {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Interrupted,
                format!("netstack tcp_shutdown tx error {}", err),
            )))
        } else {
            me.closed = true;
            Poll::Ready(Ok(()))
        }
    }
}
