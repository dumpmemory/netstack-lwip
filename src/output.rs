use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::mpsc::Sender;

use super::lwip::*;

/// Pointer (as usize) to the heap-allocated `Sender` the lwIP output callback
/// forwards packets to; 0 when no `NetStack` is alive. All accesses happen
/// under LWIP_MUTEX; the atomic just avoids `static mut`.
pub static OUTPUT_CB_PTR: AtomicUsize = AtomicUsize::new(0);

fn output(_netif: *mut netif, p: *mut pbuf) -> err_t {
    unsafe {
        let pbuflen = std::ptr::read_unaligned(p).tot_len;
        let mut buf = Vec::with_capacity(pbuflen as usize);
        pbuf_copy_partial(p, buf.as_mut_ptr() as *mut _, pbuflen, 0);
        buf.set_len(pbuflen as usize);
        let ptr = OUTPUT_CB_PTR.load(Ordering::Acquire);
        if ptr == 0 {
            return err_enum_t_ERR_ABRT as err_t;
        }
        // SAFETY: lwIP invokes this only while LWIP_MUTEX is held, and
        // StackHandle::drop clears OUTPUT_CB_PTR (and frees the Sender) under
        // the same lock, so the pointer is valid here. `Sender` is `Sync` and
        // we only take a shared reference, so this never aliases the `&mut
        // NetStack` formed in `poll_next`. A full channel drops the packet;
        // lwIP/TCP will retransmit as needed.
        let tx = &*(ptr as *const Sender<Vec<u8>>);
        if tx.try_send(buf).is_err() {
            log::trace!("netstack output channel full, dropping outbound packet");
        }
        err_enum_t_ERR_OK as err_t
    }
}

#[allow(unused_variables)]
pub extern "C" fn output_ip4(netif: *mut netif, p: *mut pbuf, ipaddr: *const ip4_addr_t) -> err_t {
    output(netif, p)
}

#[allow(unused_variables)]
#[allow(unused)]
pub extern "C" fn output_ip6(netif: *mut netif, p: *mut pbuf, ipaddr: *const ip6_addr_t) -> err_t {
    output(netif, p)
}
