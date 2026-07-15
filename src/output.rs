use tokio::sync::mpsc::Sender;

use super::lwip::*;

pub static mut OUTPUT_CB_PTR: usize = 0x0;

fn output(_netif: *mut netif, p: *mut pbuf) -> err_t {
    unsafe {
        let pbuflen = std::ptr::read_unaligned(p).tot_len;
        let mut buf = Vec::with_capacity(pbuflen as usize);
        pbuf_copy_partial(p, buf.as_mut_ptr() as *mut _, pbuflen, 0);
        buf.set_len(pbuflen as usize);
        if OUTPUT_CB_PTR == 0x0 {
            return err_enum_t_ERR_ABRT as err_t;
        }
        // SAFETY: lwIP invokes this only while LWIP_MUTEX is held, and
        // NetStack::drop clears OUTPUT_CB_PTR (and frees the Sender) under the
        // same lock, so the pointer is valid here. `Sender` is `Sync` and we
        // only take a shared reference, so this never aliases the `&mut
        // NetStack` formed in `poll_next`. A full channel drops the packet;
        // lwIP/TCP will retransmit as needed.
        let tx = &*(OUTPUT_CB_PTR as *const Sender<Vec<u8>>);
        let _ = tx.try_send(buf);
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
