//! The instance's end of an RDMA data channel (experimental).
//!
//! The manager does every transfer: it writes a claim's windows into slots of this
//! process's staging and reads a put's windows out of them, with one-sided RDMA on
//! a reliable-connected queue pair made for the transfer. This side only registers
//! its staging once, makes a queue pair per transfer, brings it up against the
//! manager's description and tears it down; it posts no work. Tearing it down is
//! the fence: moving the pair to the error state and destroying it stops every
//! packet the manager might still send for it, so nothing lands in a slot after it
//! is gone.
//!
//! The description exchanged is [`Handshake`]. It is the one the manager's data
//! channel module uses between its own ends, with the arena fields unused.

use std::{io, sync::Arc};

/// Version, qpn, psn, gid, lid, both rkeys and both base addresses.
pub const HANDSHAKE_BYTES: usize = 4 + 4 + 4 + 16 + 2 + 4 + 4 + 8 + 8;
pub const HANDSHAKE_VERSION: u32 = 1;

/// One end of a queue pair and the registered memory the other end may address.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Handshake {
    pub qpn: u32,
    pub psn: u32,
    pub gid: [u8; 16],
    pub lid: u16,
    pub rkey_arena: u32,
    pub rkey_staging: u32,
    pub arena_base: u64,
    pub staging_base: u64,
}

impl Handshake {
    pub fn encode(&self) -> [u8; HANDSHAKE_BYTES] {
        let mut out = [0u8; HANDSHAKE_BYTES];
        let mut at = 0;
        let mut put = |bytes: &[u8]| {
            out[at..at + bytes.len()].copy_from_slice(bytes);
            at += bytes.len();
        };
        put(&HANDSHAKE_VERSION.to_le_bytes());
        put(&self.qpn.to_le_bytes());
        put(&self.psn.to_le_bytes());
        put(&self.gid);
        put(&self.lid.to_le_bytes());
        put(&self.rkey_arena.to_le_bytes());
        put(&self.rkey_staging.to_le_bytes());
        put(&self.arena_base.to_le_bytes());
        put(&self.staging_base.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() != HANDSHAKE_BYTES {
            return Err(invalid("an RDMA handshake has the wrong length"));
        }
        if u32::from_le_bytes(bytes[0..4].try_into().unwrap()) != HANDSHAKE_VERSION {
            return Err(invalid("an unsupported RDMA handshake version"));
        }
        let mut gid = [0u8; 16];
        gid.copy_from_slice(&bytes[12..28]);
        Ok(Self {
            qpn: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            psn: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            gid,
            lid: u16::from_le_bytes(bytes[28..30].try_into().unwrap()),
            rkey_arena: u32::from_le_bytes(bytes[30..34].try_into().unwrap()),
            rkey_staging: u32::from_le_bytes(bytes[34..38].try_into().unwrap()),
            arena_base: u64::from_le_bytes(bytes[38..46].try_into().unwrap()),
            staging_base: u64::from_le_bytes(bytes[46..54].try_into().unwrap()),
        })
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}

#[cfg(feature = "rdma")]
mod verbs {
    use super::*;
    use ibverbs_sys as ffi;
    use std::ptr;

    const PORT: u8 = 1;
    const CQ_ENTRIES: i32 = 16;
    const PSN: u32 = 0x1234_5678;

    /// A device context with the staging region registered for remote access.
    pub struct Device {
        context: *mut ffi::ibv_context,
        pd: *mut ffi::ibv_pd,
        mr: *mut ffi::ibv_mr,
        lid: u16,
        gid: [u8; 16],
        base: u64,
    }

    // SAFETY: libibverbs objects may be used from any thread; each pair is used by one task.
    unsafe impl Send for Device {}
    // SAFETY: as above, and the device itself is never mutated after `open`.
    unsafe impl Sync for Device {}

    impl Device {
        /// Open the first device whose port is active and register `[base, base + len)`.
        ///
        /// # Safety
        ///
        /// The range must be mapped, and stay mapped and at this address, until the device
        /// is dropped.
        pub unsafe fn open(base: *mut u8, len: usize) -> io::Result<Arc<Self>> {
            let mut count = 0i32;
            // SAFETY: returns a null-terminated array owned by libibverbs.
            let list = unsafe { ffi::ibv_get_device_list(&mut count) };
            if list.is_null() {
                return Err(io::Error::last_os_error());
            }
            let mut found = None;
            for index in 0..count as usize {
                // SAFETY: the array has `count` entries; entries may be null.
                let device = unsafe { *list.add(index) };
                if device.is_null() {
                    continue;
                }
                // SAFETY: `device` came from the list, which is freed after the loop.
                let context = unsafe { ffi::ibv_open_device(device) };
                if context.is_null() {
                    continue;
                }
                let mut port = ffi::ibv_port_attr::default();
                // SAFETY: the same cast libibverbs' inline wrapper performs.
                let status = unsafe {
                    ffi::ibv_query_port(
                        context,
                        PORT,
                        &mut port as *mut ffi::ibv_port_attr as *mut ffi::_compat_ibv_port_attr,
                    )
                };
                if status == 0 && port.state == ffi::ibv_port_state::IBV_PORT_ACTIVE {
                    found = Some((context, port.lid));
                    break;
                }
                // SAFETY: opened above, not used again.
                unsafe { ffi::ibv_close_device(context) };
            }
            // SAFETY: every device we mean to use was opened before this.
            unsafe { ffi::ibv_free_device_list(list) };
            let (context, lid) = found.ok_or_else(|| invalid("no active RDMA device"))?;
            let fail = |error: io::Error, pd: *mut ffi::ibv_pd| -> io::Error {
                // SAFETY: both handles were made here and are released once.
                unsafe {
                    if !pd.is_null() {
                        ffi::ibv_dealloc_pd(pd);
                    }
                    ffi::ibv_close_device(context);
                }
                error
            };
            // SAFETY: the context is open.
            let pd = unsafe { ffi::ibv_alloc_pd(context) };
            if pd.is_null() {
                return Err(fail(io::Error::last_os_error(), ptr::null_mut()));
            }
            let access = ffi::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0
                | ffi::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE.0
                | ffi::ibv_access_flags::IBV_ACCESS_REMOTE_READ.0;
            // SAFETY: the caller guarantees the range stays mapped.
            let mr = unsafe { ffi::ibv_reg_mr(pd, base.cast(), len, access as i32) };
            if mr.is_null() {
                return Err(fail(io::Error::last_os_error(), pd));
            }
            let mut gid = unsafe { std::mem::zeroed::<ffi::ibv_gid>() };
            // SAFETY: valid context and port; index 0.
            if unsafe { ffi::ibv_query_gid(context, PORT, 0, &mut gid) } != 0 {
                // SAFETY: registered above.
                unsafe { ffi::ibv_dereg_mr(mr) };
                return Err(fail(io::Error::last_os_error(), pd));
            }
            Ok(Arc::new(Self {
                context,
                pd,
                mr,
                lid,
                // SAFETY: filled by `ibv_query_gid`.
                gid: unsafe { gid.raw },
                base: base as u64,
            }))
        }
    }

    impl Drop for Device {
        fn drop(&mut self) {
            // SAFETY: each handle is live, released once, in the order libibverbs requires.
            unsafe {
                ffi::ibv_dereg_mr(self.mr);
                ffi::ibv_dealloc_pd(self.pd);
                ffi::ibv_close_device(self.context);
            }
        }
    }

    /// A queue pair for one transfer, in INIT until connected.
    pub struct Pair {
        cq: *mut ffi::ibv_cq,
        qp: *mut ffi::ibv_qp,
        device: Arc<Device>,
    }

    // SAFETY: a pair is moved into one task and used there.
    unsafe impl Send for Pair {}
    // SAFETY: its methods take `&self` and libibverbs allows concurrent use of a queue pair.
    unsafe impl Sync for Pair {}

    impl Pair {
        pub fn new(device: &Arc<Device>) -> io::Result<Self> {
            // SAFETY: the context is open; no completion channel is used.
            let cq = unsafe {
                ffi::ibv_create_cq(
                    device.context,
                    CQ_ENTRIES,
                    ptr::null_mut(),
                    ptr::null_mut(),
                    0,
                )
            };
            if cq.is_null() {
                return Err(io::Error::last_os_error());
            }
            let mut init = ffi::ibv_qp_init_attr {
                send_cq: cq,
                recv_cq: cq,
                cap: ffi::ibv_qp_cap {
                    max_send_wr: 1,
                    max_recv_wr: 1,
                    max_send_sge: 1,
                    max_recv_sge: 1,
                    ..Default::default()
                },
                qp_type: ffi::ibv_qp_type::IBV_QPT_RC,
                sq_sig_all: 0,
                ..Default::default()
            };
            // SAFETY: the PD is live and `init` describes a valid RC queue pair.
            let qp = unsafe { ffi::ibv_create_qp(device.pd, &mut init) };
            if qp.is_null() {
                let error = io::Error::last_os_error();
                // SAFETY: made above.
                unsafe { ffi::ibv_destroy_cq(cq) };
                return Err(error);
            }
            let pair = Self {
                cq,
                qp,
                device: device.clone(),
            };
            let access = ffi::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0
                | ffi::ibv_access_flags::IBV_ACCESS_REMOTE_WRITE.0
                | ffi::ibv_access_flags::IBV_ACCESS_REMOTE_READ.0;
            let mut attr = ffi::ibv_qp_attr {
                qp_state: ffi::ibv_qp_state::IBV_QPS_INIT,
                pkey_index: 0,
                port_num: PORT,
                qp_access_flags: access,
                ..Default::default()
            };
            let mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE.0
                | ffi::ibv_qp_attr_mask::IBV_QP_PKEY_INDEX.0
                | ffi::ibv_qp_attr_mask::IBV_QP_PORT.0
                | ffi::ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS.0;
            // SAFETY: the pair is in RESET and the attributes are valid for INIT.
            if unsafe { ffi::ibv_modify_qp(pair.qp, &mut attr, mask as i32) } != 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(pair)
        }

        /// This end's description: the queue pair and the registered staging.
        pub fn handshake(&self) -> Handshake {
            Handshake {
                qpn: unsafe { (*self.qp).qp_num },
                psn: PSN,
                gid: self.device.gid,
                lid: self.device.lid,
                rkey_arena: 0,
                rkey_staging: unsafe { (*self.device.mr).rkey },
                arena_base: 0,
                staging_base: self.device.base,
            }
        }

        /// Bring the pair up against the manager's description.
        pub fn connect(&self, peer: &Handshake) -> io::Result<()> {
            let mut attr = ffi::ibv_qp_attr {
                qp_state: ffi::ibv_qp_state::IBV_QPS_RTR,
                path_mtu: ffi::IBV_MTU_1024,
                dest_qp_num: peer.qpn,
                rq_psn: peer.psn,
                max_dest_rd_atomic: 1,
                min_rnr_timer: 12,
                ah_attr: ffi::ibv_ah_attr {
                    dlid: peer.lid,
                    port_num: PORT,
                    is_global: u8::from(peer.gid != [0; 16]),
                    grh: ffi::ibv_global_route {
                        dgid: ffi::ibv_gid { raw: peer.gid },
                        sgid_index: 0,
                        hop_limit: 1,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                ..Default::default()
            };
            let mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE.0
                | ffi::ibv_qp_attr_mask::IBV_QP_AV.0
                | ffi::ibv_qp_attr_mask::IBV_QP_PATH_MTU.0
                | ffi::ibv_qp_attr_mask::IBV_QP_DEST_QPN.0
                | ffi::ibv_qp_attr_mask::IBV_QP_RQ_PSN.0
                | ffi::ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC.0
                | ffi::ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER.0;
            // SAFETY: the pair is in INIT and the RTR attributes are valid.
            if unsafe { ffi::ibv_modify_qp(self.qp, &mut attr, mask as i32) } != 0 {
                return Err(io::Error::last_os_error());
            }
            let mut attr = ffi::ibv_qp_attr {
                qp_state: ffi::ibv_qp_state::IBV_QPS_RTS,
                timeout: 14,
                retry_cnt: 7,
                rnr_retry: 7,
                sq_psn: PSN,
                max_rd_atomic: 1,
                ..Default::default()
            };
            let mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE.0
                | ffi::ibv_qp_attr_mask::IBV_QP_TIMEOUT.0
                | ffi::ibv_qp_attr_mask::IBV_QP_RETRY_CNT.0
                | ffi::ibv_qp_attr_mask::IBV_QP_RNR_RETRY.0
                | ffi::ibv_qp_attr_mask::IBV_QP_SQ_PSN.0
                | ffi::ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC.0;
            // SAFETY: the pair is in RTR and the RTS attributes are valid.
            if unsafe { ffi::ibv_modify_qp(self.qp, &mut attr, mask as i32) } != 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }
    }

    impl Drop for Pair {
        fn drop(&mut self) {
            let mut attr = ffi::ibv_qp_attr {
                qp_state: ffi::ibv_qp_state::IBV_QPS_ERR,
                ..Default::default()
            };
            // SAFETY: the pair is live; a move to the error state is always allowed. Once
            // it is there and destroyed, no packet for it reaches the registered memory.
            unsafe {
                ffi::ibv_modify_qp(
                    self.qp,
                    &mut attr,
                    ffi::ibv_qp_attr_mask::IBV_QP_STATE.0 as i32,
                );
                ffi::ibv_destroy_qp(self.qp);
                ffi::ibv_destroy_cq(self.cq);
            }
        }
    }
}

#[cfg(feature = "rdma")]
pub use verbs::{Device, Pair};

/// Without the `rdma` feature there is no device: opening one says so.
#[cfg(not(feature = "rdma"))]
mod absent {
    use super::*;

    pub struct Device;

    impl Device {
        /// # Safety
        ///
        /// Never registers anything.
        pub unsafe fn open(_: *mut u8, _: usize) -> io::Result<Arc<Self>> {
            Err(invalid("this build has no RDMA support"))
        }
    }

    pub struct Pair;

    impl Pair {
        pub fn new(_: &Arc<Device>) -> io::Result<Self> {
            Err(invalid("this build has no RDMA support"))
        }

        pub fn handshake(&self) -> Handshake {
            unreachable!("no pair exists without RDMA support")
        }

        pub fn connect(&self, _: &Handshake) -> io::Result<()> {
            unreachable!("no pair exists without RDMA support")
        }
    }
}

#[cfg(not(feature = "rdma"))]
pub use absent::{Device, Pair};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_handshake_round_trips_and_refuses_the_wrong_length_or_version() {
        let value = Handshake {
            qpn: 17,
            psn: 0x1234_5678,
            gid: [0xab; 16],
            lid: 0x0102,
            rkey_arena: 0,
            rkey_staging: 0x3333_4444,
            arena_base: 0,
            staging_base: 0x7f00_1000_0000,
        };
        let bytes = value.encode();
        assert_eq!(Handshake::decode(&bytes).unwrap(), value);
        assert!(Handshake::decode(&bytes[..HANDSHAKE_BYTES - 1]).is_err());
        let mut other = bytes;
        other[..4].copy_from_slice(&2u32.to_le_bytes());
        assert!(Handshake::decode(&other).is_err());
    }
}
