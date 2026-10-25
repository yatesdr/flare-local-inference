//! libibverbs transport glue.
//!
//! A [`Device`] registers both region files once on the first active device. Each transfer
//! takes a [`LocalRdma`] from it: an RC queue pair in INIT, with its own completion queue.
//! After the two ends exchange handshakes, `connect` moves the pair through RTR and RTS, and
//! the resulting `RdmaChannel` posts one-sided RDMA writes and reads. Dropping the channel
//! moves the pair to the error state and destroys it; the registered regions stay.
//!
//! The unsafe surface is limited to the libibverbs calls; each block has a safety comment.

use super::DataChannel;
use super::rdma::{Handshake, RdmaChannel, Remote, Verbs};
use super::{Region, RegionAccess, invalid};
use ibverbs_sys as ffi;
use std::collections::HashMap;
use std::io;
use std::mem;
use std::ptr;
use std::sync::Arc;
use std::time::{Duration, Instant};

const PORT: u8 = 1;
const CQ_ENTRIES: i32 = 128;
const SEND_WR: u32 = 64;
const PSN: u32 = 0x1234_5678;
const QUIESCE_TIMEOUT: Duration = Duration::from_secs(2);

/// A queue pair in INIT on a [`Device`], ready to be connected.
pub struct LocalRdma {
    verbs: IbvVerbs,
}

impl LocalRdma {
    /// Open the first device whose port is active, register both mapped regions and make
    /// one queue pair. For a manager that serves many transfers, open a [`Device`] once
    /// and take a pair from it per transfer.
    pub fn open(regions: Arc<dyn RegionAccess>) -> io::Result<Self> {
        Device::open(regions)?.pair()
    }

    pub fn handshake(&self) -> Handshake {
        self.verbs.handshake
    }

    /// Finish the queue pair against the peer and wrap it in a data channel.
    pub fn connect(
        mut self,
        peer: Handshake,
        peer_sizes: (u64, u64),
    ) -> io::Result<Box<dyn DataChannel>> {
        self.verbs.connect(peer)?;
        Ok(Box::new(RdmaChannel::new(self.verbs, peer, peer_sizes)?))
    }
}

/// The device context, protection domain and both registered regions. Shared by every
/// queue pair made from it, and closed when the last of them is gone.
pub struct Device {
    /// Keeps the mappings alive while the memory regions refer to them.
    _regions: Arc<dyn RegionAccess>,
    sizes: (u64, u64),
    context: *mut ffi::ibv_context,
    pd: *mut ffi::ibv_pd,
    arena: *mut ffi::ibv_mr,
    staging: *mut ffi::ibv_mr,
    lid: u16,
    gid: [u8; 16],
    /// The index of `gid` in the port's table, the source GID of every pair.
    gid_index: u32,
}

// SAFETY: libibverbs objects may be used from any thread; the handles are never changed after
// `open` and are released once, when the last queue pair and the device itself are gone.
unsafe impl Send for Device {}
// SAFETY: as above.
unsafe impl Sync for Device {}

impl Device {
    /// Open the first device whose port is active and register both mapped regions.
    ///
    /// The device retains `regions` for its whole lifetime, so the mappings stay alive while the
    /// memory regions refer to them, and every bound is taken from the registered lengths rather
    /// than from a caller-supplied size.
    pub fn open(regions: Arc<dyn RegionAccess>) -> io::Result<Arc<Self>> {
        let arena = regions
            .mapping(Region::Arena)
            .ok_or_else(|| invalid("the arena region is not mapped"))?;
        let staging = regions
            .mapping(Region::Staging)
            .ok_or_else(|| invalid("the staging region is not mapped"))?;
        if arena.1 == 0 || staging.1 == 0 || arena.0.is_null() || staging.0.is_null() {
            return Err(invalid("both RDMA regions must be mapped"));
        }
        let (context, lid) = open_active_device()?;
        match Self::register(context, lid, regions, arena, staging) {
            Ok(device) => Ok(Arc::new(device)),
            Err(error) => {
                // SAFETY: `register` released every handle it took except the device context,
                // which came from `ibv_open_device` and is owned by this function.
                unsafe {
                    ffi::ibv_close_device(context);
                }
                Err(error)
            }
        }
    }

    fn register(
        context: *mut ffi::ibv_context,
        lid: u16,
        regions: Arc<dyn RegionAccess>,
        arena: (*mut u8, usize),
        staging: (*mut u8, usize),
    ) -> io::Result<Self> {
        // SAFETY: the context is open and owned by the caller.
        let pd = unsafe { ffi::ibv_alloc_pd(context) };
        if pd.is_null() {
            return Err(io::Error::last_os_error());
        }
        let access = ffi::ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0 as i32;
        // SAFETY: both ranges are mapped by `regions`, which this value retains.
        let arena_mr = unsafe { ffi::ibv_reg_mr(pd, arena.0.cast(), arena.1, access) };
        if arena_mr.is_null() {
            let error = io::Error::last_os_error();
            release_device(pd, ptr::null_mut(), ptr::null_mut());
            return Err(error);
        }
        // SAFETY: as above for the staging range.
        let staging_mr = unsafe { ffi::ibv_reg_mr(pd, staging.0.cast(), staging.1, access) };
        if staging_mr.is_null() {
            let error = io::Error::last_os_error();
            release_device(pd, arena_mr, ptr::null_mut());
            return Err(error);
        }
        // SAFETY: the context is open.
        let (gid_index, gid) = match unsafe { flare_selene::pool::rdma::pick_gid(context, PORT) } {
            Ok(picked) => picked,
            Err(error) => {
                release_device(pd, arena_mr, staging_mr);
                return Err(error);
            }
        };
        Ok(Self {
            _regions: regions,
            sizes: (arena.1 as u64, staging.1 as u64),
            context,
            pd,
            arena: arena_mr,
            staging: staging_mr,
            lid,
            gid,
            gid_index,
        })
    }

    /// A queue pair for one transfer, in INIT.
    pub fn pair(self: &Arc<Self>) -> io::Result<LocalRdma> {
        Ok(LocalRdma {
            verbs: IbvVerbs::build(self)?,
        })
    }
}

impl super::channel::Rdma for Arc<Device> {
    fn pair(&self) -> io::Result<Box<dyn super::channel::Pair>> {
        Ok(Box::new(Device::pair(self)?))
    }
}

impl super::channel::Pair for LocalRdma {
    fn handshake(&self) -> Vec<u8> {
        self.verbs.handshake.encode().to_vec()
    }

    fn connect(self: Box<Self>, peer: &[u8]) -> io::Result<Box<dyn DataChannel>> {
        let peer = Handshake::decode(peer)?;
        if peer.rkey(Region::Staging).is_none() {
            return Err(invalid("the instance registered no staging"));
        }
        // The instance's staging is bounded by its registration: the device refuses an
        // access outside it, so no size is taken on its word.
        (*self).connect(peer, (0, u64::MAX / 2))
    }
}

impl Drop for Device {
    fn drop(&mut self) {
        release_device(self.pd, self.arena, self.staging);
        // SAFETY: the context was opened by `Device::open` and is closed exactly once here.
        unsafe {
            ffi::ibv_close_device(self.context);
        }
    }
}

/// Release the memory regions and the protection domain; the device context stays with its owner.
fn release_device(pd: *mut ffi::ibv_pd, arena: *mut ffi::ibv_mr, staging: *mut ffi::ibv_mr) {
    // SAFETY: each pointer is either null or a live libibverbs handle released exactly once.
    unsafe {
        if !arena.is_null() {
            ffi::ibv_dereg_mr(arena);
        }
        if !staging.is_null() {
            ffi::ibv_dereg_mr(staging);
        }
        if !pd.is_null() {
            ffi::ibv_dealloc_pd(pd);
        }
    }
}

fn open_active_device() -> io::Result<(*mut ffi::ibv_context, u16)> {
    let mut count = 0i32;
    // SAFETY: `ibv_get_device_list` returns a null-terminated array owned by libibverbs.
    let list = unsafe { ffi::ibv_get_device_list(&mut count) };
    if list.is_null() {
        return Err(io::Error::last_os_error());
    }
    let mut chosen = None;
    for index in 0..count as usize {
        // SAFETY: the array has `count` entries; entries may be null.
        let device = unsafe { *list.add(index) };
        if device.is_null() {
            continue;
        }
        // SAFETY: `device` came from the list and has not been freed yet.
        let context = unsafe { ffi::ibv_open_device(device) };
        if context.is_null() {
            continue;
        }
        let mut port = ffi::ibv_port_attr::default();
        // SAFETY: the compat cast is the same one libibverbs' inline wrapper performs.
        let status = unsafe {
            ffi::ibv_query_port(
                context,
                PORT,
                &mut port as *mut ffi::ibv_port_attr as *mut ffi::_compat_ibv_port_attr,
            )
        };
        if status == 0 && port.state == ffi::ibv_port_state::IBV_PORT_ACTIVE {
            chosen = Some((context, port.lid));
            break;
        }
        // SAFETY: this context was opened here and is not used again.
        unsafe {
            ffi::ibv_close_device(context);
        }
    }
    // SAFETY: every device we intend to use was opened before this call.
    unsafe {
        ffi::ibv_free_device_list(list);
    }
    chosen.ok_or_else(|| invalid("no active RDMA device"))
}

struct IbvVerbs {
    /// Keeps the device, and with it the registered regions, alive.
    device: Arc<Device>,
    cq: *mut ffi::ibv_cq,
    qp: *mut ffi::ibv_qp,
    handshake: Handshake,
    transfers: HashMap<u64, u64>,
    outstanding: HashMap<u64, u32>,
}

// SAFETY: every handle is used from the single task that owns the channel.
unsafe impl Send for IbvVerbs {}

impl IbvVerbs {
    fn build(device: &Arc<Device>) -> io::Result<Self> {
        // SAFETY: the PD is valid; the completion queue has no context or channel.
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
                max_send_wr: SEND_WR,
                max_recv_wr: 1,
                max_send_sge: 1,
                max_recv_sge: 1,
                ..Default::default()
            },
            qp_type: ffi::ibv_qp_type::IBV_QPT_RC,
            sq_sig_all: 0,
            ..Default::default()
        };
        // SAFETY: the PD is valid and `init` describes a valid RC queue pair.
        let qp = unsafe { ffi::ibv_create_qp(device.pd, &mut init) };
        if qp.is_null() {
            let error = io::Error::last_os_error();
            release_pair(ptr::null_mut(), cq);
            return Err(error);
        }
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_INIT,
            pkey_index: 0,
            port_num: PORT,
            qp_access_flags: 0,
            ..Default::default()
        };
        let mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE.0
            | ffi::ibv_qp_attr_mask::IBV_QP_PKEY_INDEX.0
            | ffi::ibv_qp_attr_mask::IBV_QP_PORT.0
            | ffi::ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS.0;
        // SAFETY: the queue pair is in RESET and the attributes are valid for INIT.
        let status = unsafe { ffi::ibv_modify_qp(qp, &mut attr, mask as i32) };
        if status != 0 {
            let error = io::Error::last_os_error();
            release_pair(qp, cq);
            return Err(error);
        }
        Ok(Self {
            device: device.clone(),
            cq,
            qp,
            handshake: Handshake {
                qpn: unsafe { (*qp).qp_num },
                psn: PSN,
                gid: device.gid,
                lid: device.lid,
                rkey_arena: 0,
                rkey_staging: 0,
                arena_base: 0,
                staging_base: 0,
            },
            transfers: HashMap::new(),
            outstanding: HashMap::new(),
        })
    }

    fn connect(&mut self, peer: Handshake) -> io::Result<()> {
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
                    sgid_index: self.device.gid_index as u8,
                    hop_limit: 64,
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
        // SAFETY: the queue pair is in INIT and the RTR attributes are valid.
        let status = unsafe { ffi::ibv_modify_qp(self.qp, &mut attr, mask as i32) };
        if status != 0 {
            return Err(io::Error::last_os_error());
        }
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_RTS,
            timeout: 14,
            retry_cnt: 7,
            rnr_retry: 7,
            sq_psn: self.handshake.psn,
            max_rd_atomic: 1,
            ..Default::default()
        };
        let mask = ffi::ibv_qp_attr_mask::IBV_QP_STATE.0
            | ffi::ibv_qp_attr_mask::IBV_QP_TIMEOUT.0
            | ffi::ibv_qp_attr_mask::IBV_QP_RETRY_CNT.0
            | ffi::ibv_qp_attr_mask::IBV_QP_RNR_RETRY.0
            | ffi::ibv_qp_attr_mask::IBV_QP_SQ_PSN.0
            | ffi::ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC.0;
        // SAFETY: the queue pair is in RTR and the RTS attributes are valid.
        let status = unsafe { ffi::ibv_modify_qp(self.qp, &mut attr, mask as i32) };
        if status != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn region_mr(&self, region: Region) -> *mut ffi::ibv_mr {
        match region {
            Region::Arena => self.device.arena,
            Region::Staging => self.device.staging,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn post(
        &mut self,
        transfer: u64,
        id: u64,
        local_key: u32,
        local_offset: u64,
        len: usize,
        remote: Remote,
        read: bool,
    ) -> io::Result<()> {
        let mr = if unsafe { (*self.device.arena).lkey } == local_key {
            self.device.arena
        } else {
            self.device.staging
        };
        // SAFETY: the MR is registered for the channel's lifetime.
        let base = unsafe { (*mr).addr as u64 };
        let mut sge = ffi::ibv_sge {
            addr: base
                .checked_add(local_offset)
                .ok_or_else(|| invalid("local address overflow"))?,
            length: len as u32,
            lkey: local_key,
        };
        let mut wr = unsafe { mem::zeroed::<ffi::ibv_send_wr>() };
        wr.wr_id = id;
        wr.sg_list = &mut sge;
        wr.num_sge = 1;
        wr.opcode = if read {
            ffi::ibv_wr_opcode::IBV_WR_RDMA_READ
        } else {
            ffi::ibv_wr_opcode::IBV_WR_RDMA_WRITE
        };
        wr.send_flags = ffi::ibv_send_flags::IBV_SEND_SIGNALED.0;
        wr.wr.rdma = ffi::ibv_send_wr__bindgen_ty_2__bindgen_ty_1 {
            remote_addr: remote.address,
            rkey: remote.rkey,
        };
        let mut bad: *mut ffi::ibv_send_wr = ptr::null_mut();
        // SAFETY: the queue pair is RTS and the work request refers to registered memory.
        let errno = unsafe {
            let ops = &mut (*self.qp).context.as_mut().unwrap().ops;
            (ops.post_send
                .ok_or_else(|| invalid("libibverbs has no post_send"))?)(
                self.qp, &mut wr, &mut bad
            )
        };
        if errno != 0 {
            return Err(io::Error::from_raw_os_error(errno));
        }
        self.transfers.insert(id, transfer);
        *self.outstanding.entry(transfer).or_insert(0) += 1;
        Ok(())
    }

    /// Poll the completion queue. Each entry carries the transfer that posted it, so a caller can
    /// filter without consulting state that this method already removed.
    fn reap(&mut self, done: &mut Vec<(u64, u64, io::Result<()>)>) -> io::Result<()> {
        let poll = unsafe {
            (*self.qp)
                .context
                .as_mut()
                .unwrap()
                .ops
                .poll_cq
                .ok_or_else(|| invalid("libibverbs has no poll_cq"))?
        };
        let mut completions = unsafe { mem::zeroed::<[ffi::ibv_wc; 16]>() };
        // SAFETY: the CQ is valid and the array holds 16 entries.
        let count = unsafe { poll(self.cq, 16, completions.as_mut_ptr()) };
        if count < 0 {
            return Err(io::Error::last_os_error());
        }
        for completion in completions.iter().take(count as usize) {
            let id = completion.wr_id();
            let result = if completion.is_valid() {
                Ok(())
            } else {
                Err(io::Error::other("RDMA work completion failed"))
            };
            let owner = self.transfers.remove(&id).unwrap_or(0);
            if let Some(remaining) = self.outstanding.get_mut(&owner) {
                *remaining = remaining.saturating_sub(1);
                if *remaining == 0 {
                    self.outstanding.remove(&owner);
                }
            }
            done.push((id, owner, result));
        }
        Ok(())
    }
}

impl Verbs for IbvVerbs {
    fn memory_key(&self, region: Region) -> io::Result<u32> {
        // A local SGE carries the local key; the rkey is only for remote addressing.
        // SAFETY: the MR is registered for the channel's lifetime.
        Ok(unsafe { (*self.region_mr(region)).lkey })
    }

    fn local_size(&self, region: Region) -> u64 {
        match region {
            Region::Arena => self.device.sizes.0,
            Region::Staging => self.device.sizes.1,
        }
    }

    fn post_write(
        &mut self,
        transfer: u64,
        id: u64,
        local_key: u32,
        local_offset: u64,
        len: usize,
        remote: Remote,
    ) -> io::Result<()> {
        self.post(transfer, id, local_key, local_offset, len, remote, false)
    }

    fn post_read(
        &mut self,
        transfer: u64,
        id: u64,
        local_key: u32,
        local_offset: u64,
        len: usize,
        remote: Remote,
    ) -> io::Result<()> {
        self.post(transfer, id, local_key, local_offset, len, remote, true)
    }

    fn poll(&mut self, done: &mut Vec<(u64, io::Result<()>)>) -> io::Result<()> {
        let mut owned = Vec::new();
        self.reap(&mut owned)?;
        done.extend(owned.into_iter().map(|(id, _, result)| (id, result)));
        Ok(())
    }

    fn quiesce(
        &mut self,
        transfer: u64,
        drained: &mut Vec<(u64, io::Result<()>)>,
    ) -> io::Result<()> {
        let deadline = Instant::now() + QUIESCE_TIMEOUT;
        loop {
            if self.outstanding.get(&transfer).copied().unwrap_or(0) == 0 {
                return Ok(());
            }
            let mut completions = Vec::new();
            self.reap(&mut completions)?;
            for (id, owner, result) in completions {
                if owner == transfer {
                    // The channel already failed this window; discard its drained completion.
                    continue;
                }
                drained.push((id, result));
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "RDMA quiesce did not drain",
                ));
            }
            std::thread::sleep(Duration::from_micros(50));
        }
    }

    fn abort(&mut self) {
        // Moving the queue pair to ERROR fails every outstanding work request, so the following
        // drain sees a completion for each one and no DMA can touch the region afterwards.
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_ERR,
            ..Default::default()
        };
        // SAFETY: the queue pair is live; a transition to ERROR is always permitted.
        unsafe {
            ffi::ibv_modify_qp(
                self.qp,
                &mut attr,
                ffi::ibv_qp_attr_mask::IBV_QP_STATE.0 as i32,
            );
        }
        let deadline = Instant::now() + QUIESCE_TIMEOUT;
        while !self.outstanding.is_empty() && Instant::now() < deadline {
            let mut owned = Vec::new();
            if self.reap(&mut owned).is_err() {
                break;
            }
            if owned.is_empty() {
                std::thread::sleep(Duration::from_micros(50));
            }
        }
        self.transfers.clear();
        self.outstanding.clear();
    }
}

impl Drop for IbvVerbs {
    fn drop(&mut self) {
        // The error state flushes every request still on the queue and stops the peer's packets
        // for this pair; destroying it then leaves nothing that can touch the regions.
        let mut attr = ffi::ibv_qp_attr {
            qp_state: ffi::ibv_qp_state::IBV_QPS_ERR,
            ..Default::default()
        };
        // SAFETY: the pair is live; a transition to ERROR is always permitted.
        unsafe {
            ffi::ibv_modify_qp(
                self.qp,
                &mut attr,
                ffi::ibv_qp_attr_mask::IBV_QP_STATE.0 as i32,
            );
        }
        release_pair(self.qp, self.cq);
        self.qp = ptr::null_mut();
        self.cq = ptr::null_mut();
    }
}

/// Destroy a pair's queue pair and completion queue; the device keeps the rest.
fn release_pair(qp: *mut ffi::ibv_qp, cq: *mut ffi::ibv_cq) {
    // SAFETY: each pointer is either null or a live libibverbs handle destroyed exactly once.
    unsafe {
        if !qp.is_null() {
            ffi::ibv_destroy_qp(qp);
        }
        if !cq.is_null() {
            ffi::ibv_destroy_cq(cq);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::alloc::{Layout, alloc_zeroed, dealloc};

    const BYTES: usize = 4 << 20;

    struct Buffer {
        pointer: *mut u8,
        layout: Layout,
    }

    // SAFETY: the test drives one buffer from one thread at a time and never resizes it.
    unsafe impl Send for Buffer {}
    // SAFETY: as above.
    unsafe impl Sync for Buffer {}

    impl Buffer {
        fn new() -> Self {
            let layout = Layout::from_size_align(BYTES, 4096).unwrap();
            // SAFETY: the layout is non-zero and the allocation is freed in Drop.
            let pointer = unsafe { alloc_zeroed(layout) };
            assert!(!pointer.is_null());
            Self { pointer, layout }
        }

        fn pointer(&self) -> *mut u8 {
            self.pointer
        }

        fn bytes(&self) -> &[u8] {
            // SAFETY: the allocation is BYTES long and owned by this value.
            unsafe { std::slice::from_raw_parts(self.pointer, BYTES) }
        }
    }

    impl Drop for Buffer {
        fn drop(&mut self) {
            // SAFETY: the pointer came from the same layout's allocation.
            unsafe {
                dealloc(self.pointer, self.layout);
            }
        }
    }

    struct TestRegions {
        arena: Buffer,
        staging: Buffer,
    }

    impl TestRegions {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                arena: Buffer::new(),
                staging: Buffer::new(),
            })
        }

        fn buffer(&self, region: Region) -> &Buffer {
            match region {
                Region::Arena => &self.arena,
                Region::Staging => &self.staging,
            }
        }
    }

    impl RegionAccess for TestRegions {
        fn size(&self, _region: Region) -> u64 {
            BYTES as u64
        }

        fn mapping(&self, region: Region) -> Option<(*mut u8, usize)> {
            Some((self.buffer(region).pointer(), BYTES))
        }

        fn with_read(
            &self,
            region: Region,
            offset: u64,
            length: usize,
            f: &mut dyn FnMut(&[u8]) -> io::Result<()>,
        ) -> io::Result<()> {
            let bytes = self.buffer(region).bytes();
            let start = usize::try_from(offset).map_err(|_| invalid("offset"))?;
            let end = start.checked_add(length).ok_or_else(|| invalid("offset"))?;
            f(bytes.get(start..end).ok_or_else(|| invalid("range"))?)
        }

        fn with_write(
            &self,
            region: Region,
            offset: u64,
            length: usize,
            f: &mut dyn FnMut(&mut [u8]) -> io::Result<()>,
        ) -> io::Result<()> {
            let start = usize::try_from(offset).map_err(|_| invalid("offset"))?;
            let end = start.checked_add(length).ok_or_else(|| invalid("offset"))?;
            if end > BYTES {
                return Err(invalid("range"));
            }
            // SAFETY: the range was checked against the allocation this value owns.
            let bytes = unsafe {
                std::slice::from_raw_parts_mut(self.buffer(region).pointer().add(start), length)
            };
            f(bytes)
        }
    }

    #[test]
    fn a_loopback_pair_moves_bytes_both_ways() {
        let regions_a = TestRegions::new();
        let regions_b = TestRegions::new();
        let Ok(device_a) = Device::open(regions_a.clone()) else {
            assert!(
                std::env::var_os("SELENE_RDMA_REQUIRED").is_none(),
                "no active RDMA device"
            );
            eprintln!("skipping: no active RDMA device");
            return;
        };
        let local_a = device_a.pair().unwrap();
        // SAFETY: this private destination stays mapped until the pair and device are dropped.
        let device_b =
            unsafe { flare_selene::pool::rdma::Device::open(regions_b.staging.pointer(), BYTES) }
                .unwrap();
        let b = flare_selene::pool::rdma::Pair::new(&device_b).unwrap();
        let handshake_a = local_a.handshake();
        assert_eq!((handshake_a.rkey_arena, handshake_a.rkey_staging), (0, 0));
        assert_eq!((handshake_a.arena_base, handshake_a.staging_base), (0, 0));
        let mut a = local_a
            .connect(
                Handshake::decode(&b.handshake().encode()).unwrap(),
                (0, BYTES as u64),
            )
            .unwrap();
        b.connect(&flare_selene::pool::rdma::Handshake::decode(&handshake_a.encode()).unwrap())
            .unwrap();
        // SAFETY: the test owns both allocations.
        unsafe {
            for index in 0..4096 {
                *regions_a.arena.pointer().add(index) = (index % 251) as u8;
            }
        }
        a.write(1, 0, Region::Arena, 0, 4096, Region::Staging, 0)
            .unwrap();
        wait(&mut a, 1, 0);
        assert_eq!(
            &regions_b.staging.bytes()[..4096],
            &regions_a.arena.bytes()[..4096]
        );
        // A multi-piece write exercises splitting and completion aggregation on the device.
        let len = 2 * (1 << 20) + 4096;
        unsafe {
            for index in 0..len {
                *regions_a.arena.pointer().add(index) = (index % 251) as u8;
            }
        }
        a.write(3, 0, Region::Arena, 0, len as u64, Region::Staging, 0)
            .unwrap();
        wait(&mut a, 3, 0);
        assert_eq!(
            &regions_b.staging.bytes()[..len],
            &regions_a.arena.bytes()[..len]
        );
        unsafe {
            for index in 4096..8192 {
                *regions_b.staging.pointer().add(index) = (index % 241) as u8;
            }
        }
        a.read(2, 0, Region::Arena, 4096, 4096, Region::Staging, 4096)
            .unwrap();
        wait(&mut a, 2, 0);
        assert_eq!(
            &regions_a.arena.bytes()[4096..8192],
            &regions_b.staging.bytes()[4096..8192]
        );
    }

    fn wait(channel: &mut Box<dyn DataChannel>, transfer: u64, index: u32) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let mut done = Vec::new();
            channel.poll(&mut done).unwrap();
            for (id, window, result) in done {
                if id == transfer && window == index {
                    result.unwrap();
                    return;
                }
            }
            assert!(Instant::now() < deadline, "RDMA transfer timed out");
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}
