//! Shared-memory lifecycle and window ownership for the manager.
//!
//! This module knows nothing about `Store`. The owning thread drives it directly:
//! accept a connection with its pid, grant and release windows, close the connection,
//! and call [`Local::poll_exits`] on each tick. Exit notifications come back as
//! connection ids through the vector passed to `poll_exits`; the caller turns them
//! into its own `ProcessExited` command.

use crate::{
    Result,
    arena::{Arena, Extent},
    invalid,
};
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{self, Read},
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
        unix::fs::{OpenOptionsExt, PermissionsExt},
    },
    path::{Path, PathBuf},
    sync::Arc,
};

pub const SHM_ROOT: &str = "/dev/shm/selene";
pub const PAGE_BYTES: u64 = 4096;
pub const MAX_WINDOW_PAGES: u32 = 64;
/// Granted windows per direction per connection.
pub const WINDOW_CREDITS: u32 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Region {
    Arena,
    Staging,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Direction {
    Put,
    Get,
}

/// One or more complete padded units: whole pages, the whole row, or both.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WindowShape {
    pub pages: u32,
    pub row: bool,
    pub page_bytes: u64,
    pub row_bytes: u64,
}

impl WindowShape {
    pub fn new(pages: u32, row: bool, page_bytes: u64, row_bytes: u64) -> Result<Self> {
        if pages > MAX_WINDOW_PAGES {
            return Err(invalid(format!(
                "a window covers at most {MAX_WINDOW_PAGES} pages, got {pages}"
            )));
        }
        if pages == 0 && !row {
            return Err(invalid("a window must cover at least one page or the row"));
        }
        if pages == 0 {
            if page_bytes != 0 {
                return Err(invalid("page_bytes without pages"));
            }
        } else if page_bytes == 0 || !page_bytes.is_multiple_of(PAGE_BYTES) {
            return Err(invalid(format!(
                "page_bytes {page_bytes} must be a positive multiple of {PAGE_BYTES}"
            )));
        }
        if row {
            if row_bytes == 0 || !row_bytes.is_multiple_of(PAGE_BYTES) {
                return Err(invalid(format!(
                    "row_bytes {row_bytes} must be a positive multiple of {PAGE_BYTES}"
                )));
            }
        } else if row_bytes != 0 {
            return Err(invalid("row_bytes without the row"));
        }
        Ok(Self {
            pages,
            row,
            page_bytes,
            row_bytes,
        })
    }

    pub fn length(&self) -> u64 {
        u64::from(self.pages) * self.page_bytes + if self.row { self.row_bytes } else { 0 }
    }

    /// `length <= max_window_bytes`, unless the window is exactly one oversized unit.
    pub fn fits(&self, max_window_bytes: u64) -> bool {
        if self.length() <= max_window_bytes {
            return true;
        }
        // Only a row-only or single-page window may exceed the negotiated maximum,
        // and then it is granted at exactly its own size.
        matches!((self.pages, self.row), (0, true) | (1, false))
    }

    pub fn validate(&self, max_window_bytes: u64) -> Result<()> {
        if !self.fits(max_window_bytes) {
            return Err(invalid(format!(
                "window of {} bytes exceeds max_window_bytes {max_window_bytes}",
                self.length()
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WindowGrant {
    pub transfer: u64,
    pub index: u32,
    pub offset: u64,
    pub length: u64,
    pub pages: u32,
    pub row: bool,
    pub region: Region,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct ConnectionStats {
    pub bytes: u64,
    pub put_windows: u32,
    pub get_windows: u32,
    /// Bytes held by a closed connection whose process has not been confirmed exited.
    pub quarantine_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GrantError {
    /// The region (or staging region) has no room; the caller answers `BUDGET`.
    Budget,
    /// Both per-direction window credits are already granted; the caller queues.
    Credits,
    Invalid(String),
}

impl std::fmt::Display for GrantError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Budget => formatter.write_str("window budget exhausted"),
            Self::Credits => formatter.write_str("window credits exhausted"),
            Self::Invalid(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for GrantError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReleaseError {
    UnknownConnection,
    UnknownWindow,
}

/// Checks a region-file range. Every window uses checked arithmetic and reports the
/// peer pid on rejection.
pub fn check_range(
    size: u64,
    offset: u64,
    length: u64,
    pid: u32,
) -> std::result::Result<(), GrantError> {
    check_range_opt(size, offset, length, Some(pid))
}

fn check_range_opt(
    size: u64,
    offset: u64,
    length: u64,
    pid: Option<u32>,
) -> std::result::Result<(), GrantError> {
    let who = pid.map_or_else(
        || "peer session".to_owned(),
        |pid| format!("peer pid {pid}"),
    );
    if !offset.is_multiple_of(PAGE_BYTES) {
        return Err(GrantError::Invalid(format!(
            "window offset {offset} is not {PAGE_BYTES}-aligned ({who})"
        )));
    }
    if !length.is_multiple_of(PAGE_BYTES) {
        return Err(GrantError::Invalid(format!(
            "window length {length} is not a multiple of {PAGE_BYTES} ({who})"
        )));
    }
    match offset.checked_add(length) {
        Some(end) if end <= size => Ok(()),
        _ => Err(GrantError::Invalid(format!(
            "window [{offset}, +{length}) exceeds region size {size} ({who})"
        ))),
    }
}

enum Hold {
    Allocated { extent: Extent },
    Lease(Arc<Extent>),
    None,
}

struct Window {
    direction: Direction,
    length: usize,
    within: usize,
    hold: Hold,
}

impl Window {
    fn take_hold(&mut self) -> Hold {
        std::mem::replace(&mut self.hold, Hold::None)
    }
}

impl Drop for Window {
    fn drop(&mut self) {
        if let Hold::Lease(extent) = &self.hold {
            extent.release();
        }
    }
}

struct Connection {
    kind: SessionKind,
    pid: Option<u32>,
    pidfd: Option<OwnedFd>,
    exited: bool,
    reported: bool,
    closed: bool,
    windows: HashMap<(u64, u32), Window>,
    armed: u64,
}

/// Who owns a session's windows. Peer sessions have no local pidfd; they are
/// reclaimed when their control/data streams close.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionKind {
    Local { pid: u32 },
    Remote,
}

/// One manager instance's shared-memory regions and the windows granted from them.
pub struct Local {
    instance: String,
    directory: PathBuf,
    boot_nonce: String,
    _lock: File,
    arena: Arena,
    staging: Arena,
    arena_path: PathBuf,
    staging_path: PathBuf,
    arena_size: u64,
    staging_size: u64,
    max_window_bytes: u64,
    connections: HashMap<u64, Connection>,
}

/// Names the startup step and path an error came from.
fn step_error(step: &str, path: &Path, error: io::Error) -> io::Error {
    io::Error::new(error.kind(), format!("{step} {}: {error}", path.display()))
}

impl Local {
    /// Owns `/dev/shm/selene/<instance>`: takes the exclusive lock, reclaims
    /// everything a crashed owner left behind and creates fresh, preallocated
    /// regions.
    pub fn open(
        instance: &str,
        arena_bytes: usize,
        staging_bytes: usize,
        max_window_bytes: u64,
    ) -> Result<Self> {
        validate_instance(instance)?;
        let root = Path::new(SHM_ROOT);
        create_private_directory(root).map_err(|error| step_error("create", root, error))?;
        let directory = root.join(instance);
        create_private_directory(&directory)
            .map_err(|error| step_error("create", &directory, error))?;

        let lock_path = directory.join("lock");
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .mode(0o660)
            .open(&lock_path)
            .map_err(|error| step_error("open", &lock_path, error))?;
        if unsafe { libc::fchmod(lock.as_raw_fd(), 0o660) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // The open flags already include O_CLOEXEC; set the descriptor flag as well so no child
        // can keep the instance lock alive across an exec.
        if unsafe { libc::fcntl(lock.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } == -1 {
            return Err(io::Error::last_os_error());
        }
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            let error = io::Error::last_os_error();
            return Err(io::Error::new(
                error.kind(),
                format!("instance '{instance}' is already in use: {error}"),
            ));
        }

        let reclaimed = reclaim_stale(&directory)
            .map_err(|error| step_error("reclaim in", &directory, error))?;
        if reclaimed > 0 {
            eprintln!("selene: reclaimed stale instance '{instance}' ({reclaimed} files)");
        }

        let boot_nonce = boot_nonce()?;
        let (arena_path, arena_size) = create_region(&directory, "arena", &boot_nonce, arena_bytes)
            .map_err(|error| step_error("create the arena in", &directory, error))?;
        let (staging_path, staging_size) =
            create_region(&directory, "staging", &boot_nonce, staging_bytes)
                .map_err(|error| step_error("create staging in", &directory, error))?;
        let arena = Arena::open(&arena_path, arena_size)?;
        let staging = Arena::open(&staging_path, staging_size)?;

        Ok(Self {
            instance: instance.to_owned(),
            directory,
            boot_nonce,
            _lock: lock,
            arena,
            staging,
            arena_path,
            staging_path,
            arena_size: arena_size as u64,
            staging_size: staging_size as u64,
            max_window_bytes,
            connections: HashMap::new(),
        })
    }

    pub fn instance(&self) -> &str {
        &self.instance
    }
    pub fn directory(&self) -> &Path {
        &self.directory
    }
    pub fn boot_nonce(&self) -> &str {
        &self.boot_nonce
    }
    pub fn arena_path(&self) -> &Path {
        &self.arena_path
    }
    pub fn staging_path(&self) -> &Path {
        &self.staging_path
    }
    pub fn arena_size(&self) -> u64 {
        self.arena_size
    }
    pub fn staging_size(&self) -> u64 {
        self.staging_size
    }
    pub fn max_window_bytes(&self) -> u64 {
        self.max_window_bytes
    }
    pub fn arena(&self) -> Arena {
        self.arena.clone()
    }
    pub fn staging(&self) -> Arena {
        self.staging.clone()
    }
    pub fn region_size(&self, region: Region) -> u64 {
        match region {
            Region::Arena => self.arena_size,
            Region::Staging => self.staging_size,
        }
    }
    /// Base pointer and full mapped length of a region. Valid for the life of the
    /// mapping and stable across allocations.
    pub fn region_mapping(&self, region: Region) -> (*mut u8, usize) {
        match region {
            Region::Arena => self.arena.mapping(),
            Region::Staging => self.staging.mapping(),
        }
    }
    pub fn arena_used(&self) -> usize {
        self.arena.used()
    }
    pub fn staging_used(&self) -> usize {
        self.staging.used()
    }

    /// Records the peer pidfd. `ESRCH` means the process already exited; any other
    /// error leaves the connection unconfirmed so its windows stay quarantined.
    pub fn accept(&mut self, connection: u64, pid: u32) -> Result<()> {
        self.accept_with(connection, SessionKind::Local { pid })
    }

    /// Registers a manager-to-manager session. It has no local pidfd; a peer's
    /// windows are reclaimed by `close`/`peer_lost`.
    pub fn accept_remote(&mut self, connection: u64) -> Result<()> {
        self.accept_with(connection, SessionKind::Remote)
    }

    fn accept_with(&mut self, connection: u64, kind: SessionKind) -> Result<()> {
        if self.connections.contains_key(&connection) {
            return Ok(());
        }
        let (pid, pidfd, exited) = match kind {
            SessionKind::Local { pid } => {
                if pid == 0 {
                    return Err(invalid("peer pid 0"));
                }
                match open_pidfd(pid) {
                    Pidfd::Open(fd) => (Some(pid), Some(fd), false),
                    Pidfd::Exited => (Some(pid), None, true),
                    Pidfd::Unknown => (Some(pid), None, false),
                }
            }
            SessionKind::Remote => (None, None, false),
        };
        self.connections.insert(
            connection,
            Connection {
                kind,
                pid,
                pidfd,
                exited,
                reported: false,
                closed: false,
                windows: HashMap::new(),
                armed: 0,
            },
        );
        Ok(())
    }

    /// Marks the transport closed. Windows are reclaimed only if the process is
    /// already confirmed exited; otherwise they remain quarantined.
    pub fn close(&mut self, connection: u64) {
        if let Some(record) = self.connections.get_mut(&connection) {
            record.closed = true;
            if record.exited || record.kind == SessionKind::Remote {
                record.exited = true;
                record.reported = true;
                record.windows.clear();
            }
        }
        self.connections
            .retain(|_, record| !(record.closed && record.exited && record.reported));
    }

    /// Releases a peer's windows when its control/data streams end.
    pub fn peer_lost(&mut self, connection: u64) {
        if let Some(record) = self.connections.get_mut(&connection) {
            record.closed = true;
            record.exited = true;
            record.windows.clear();
        }
        self.connections
            .retain(|_, record| !(record.closed && record.exited && record.reported));
    }

    /// Non-blocking pidfd poll. Each exit is reported once. Windows are reclaimed
    /// only when the connection is also closed; a live-but-disconnected process
    /// keeps them quarantined and they continue to occupy the region.
    pub fn poll_exits(&mut self, exited: &mut Vec<u64>) {
        for (connection, record) in &mut self.connections {
            if !record.exited {
                if let Some(pidfd) = &record.pidfd {
                    if pidfd_exited(pidfd) {
                        record.exited = true;
                    }
                }
            }
            if record.exited && !record.reported {
                record.reported = true;
                exited.push(*connection);
            }
        }
        self.connections
            .retain(|_, record| !(record.closed && record.exited && record.reported));
    }

    /// Grants an exclusive writable Put window from `region`.
    pub fn grant_put(
        &mut self,
        connection: u64,
        transfer: u64,
        index: u32,
        region: Region,
        shape: WindowShape,
    ) -> std::result::Result<WindowGrant, GrantError> {
        self.grant_allocated(connection, transfer, index, Direction::Put, region, shape)
    }

    /// Grants a Get window filled through staging (streaming/NVMe path).
    pub fn grant_get_staging(
        &mut self,
        connection: u64,
        transfer: u64,
        index: u32,
        shape: WindowShape,
    ) -> std::result::Result<WindowGrant, GrantError> {
        self.grant_allocated(
            connection,
            transfer,
            index,
            Direction::Get,
            Region::Staging,
            shape,
        )
    }

    /// Grants a shared immutable lease on a stored extent. The extent cannot be
    /// freed until the window is released.
    pub fn grant_get_lease(
        &mut self,
        connection: u64,
        transfer: u64,
        index: u32,
        shape: WindowShape,
        extent: &Arc<Extent>,
        within: usize,
    ) -> std::result::Result<WindowGrant, GrantError> {
        self.grant_lease(
            connection,
            transfer,
            index,
            Direction::Get,
            Region::Arena,
            shape,
            extent,
            within,
        )
    }

    /// Grants an exclusive writable window on part of an already reserved extent
    /// (the record's arena extent).
    #[allow(clippy::too_many_arguments)]
    pub fn grant_put_extent(
        &mut self,
        connection: u64,
        transfer: u64,
        index: u32,
        region: Region,
        shape: WindowShape,
        extent: &Arc<Extent>,
        within: usize,
    ) -> std::result::Result<WindowGrant, GrantError> {
        self.grant_lease(
            connection,
            transfer,
            index,
            Direction::Put,
            region,
            shape,
            extent,
            within,
        )
    }

    /// Grants a shared read lease on an already allocated extent in either region.
    #[allow(clippy::too_many_arguments)]
    pub fn grant_get_extent(
        &mut self,
        connection: u64,
        transfer: u64,
        index: u32,
        region: Region,
        shape: WindowShape,
        extent: &Arc<Extent>,
        within: usize,
    ) -> std::result::Result<WindowGrant, GrantError> {
        self.grant_lease(
            connection,
            transfer,
            index,
            Direction::Get,
            region,
            shape,
            extent,
            within,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn grant_lease(
        &mut self,
        connection: u64,
        transfer: u64,
        index: u32,
        direction: Direction,
        region: Region,
        shape: WindowShape,
        extent: &Arc<Extent>,
        within: usize,
    ) -> std::result::Result<WindowGrant, GrantError> {
        let pid = self.pid(connection)?;
        let who = Self::pid_text(pid);
        shape
            .validate(self.max_window_bytes)
            .map_err(|error| GrantError::Invalid(format!("{error} ({who})")))?;
        self.check_credit(connection, direction)?;
        self.check_new_window(connection, transfer, index, &who)?;
        let length = usize::try_from(shape.length())
            .map_err(|_| GrantError::Invalid(format!("window length does not fit ({who})")))?;
        if !within.is_multiple_of(PAGE_BYTES as usize) {
            return Err(GrantError::Invalid(format!(
                "lease offset {within} is not {PAGE_BYTES}-aligned ({who})"
            )));
        }
        let end = within
            .checked_add(length)
            .ok_or_else(|| GrantError::Invalid(format!("lease range overflows ({who})")))?;
        if end > extent.bytes().len() {
            return Err(GrantError::Invalid(format!(
                "lease [{within}, +{length}) exceeds extent length {} ({who})",
                extent.bytes().len()
            )));
        }
        let offset = extent
            .offset()
            .checked_add(within)
            .ok_or_else(|| GrantError::Invalid(format!("region offset overflows ({who})")))?
            as u64;
        check_range_opt(self.region_size(region), offset, shape.length(), pid)?;

        extent.acquire();
        self.connections
            .get_mut(&connection)
            .unwrap()
            .windows
            .insert(
                (transfer, index),
                Window {
                    direction,
                    length,
                    within,
                    hold: Hold::Lease(extent.clone()),
                },
            );
        Ok(WindowGrant {
            transfer,
            index,
            offset,
            length: shape.length(),
            pages: shape.pages,
            row: shape.row,
            region,
        })
    }

    fn grant_allocated(
        &mut self,
        connection: u64,
        transfer: u64,
        index: u32,
        direction: Direction,
        region: Region,
        shape: WindowShape,
    ) -> std::result::Result<WindowGrant, GrantError> {
        let pid = self.pid(connection)?;
        let who = Self::pid_text(pid);
        shape
            .validate(self.max_window_bytes)
            .map_err(|error| GrantError::Invalid(format!("{error} ({who})")))?;
        self.check_credit(connection, direction)?;
        self.check_new_window(connection, transfer, index, &who)?;
        let length = usize::try_from(shape.length())
            .map_err(|_| GrantError::Invalid(format!("window length does not fit ({who})")))?;
        let (size, extent) = match region {
            Region::Arena => (self.arena_size, self.arena.allocate(length)),
            Region::Staging => (self.staging_size, self.staging.allocate(length)),
        };
        let extent = extent.ok_or(GrantError::Budget)?;
        let offset = extent.offset() as u64;
        check_range_opt(size, offset, shape.length(), pid)?;
        self.connections
            .get_mut(&connection)
            .unwrap()
            .windows
            .insert(
                (transfer, index),
                Window {
                    direction,
                    length,
                    within: 0,
                    hold: Hold::Allocated { extent },
                },
            );
        Ok(WindowGrant {
            transfer,
            index,
            offset,
            length: shape.length(),
            pages: shape.pages,
            row: shape.row,
            region,
        })
    }

    /// Explicit `WindowFilled`/`WindowReleased`. Works for quarantined windows too.
    pub fn release(
        &mut self,
        connection: u64,
        transfer: u64,
        index: u32,
    ) -> std::result::Result<(), ReleaseError> {
        let record = self
            .connections
            .get_mut(&connection)
            .ok_or(ReleaseError::UnknownConnection)?;
        record
            .windows
            .remove(&(transfer, index))
            .map(|_| ())
            .ok_or(ReleaseError::UnknownWindow)
    }

    pub fn window_bytes(&self, connection: u64, transfer: u64, index: u32) -> Option<&[u8]> {
        let window = self
            .connections
            .get(&connection)?
            .windows
            .get(&(transfer, index))?;
        match &window.hold {
            Hold::Allocated { extent } => Some(extent.bytes()),
            Hold::Lease(extent) => {
                Some(&extent.bytes()[window.within..window.within + window.length])
            }
            Hold::None => None,
        }
    }

    pub fn window_bytes_mut(
        &mut self,
        connection: u64,
        transfer: u64,
        index: u32,
    ) -> Option<&mut [u8]> {
        let window = self
            .connections
            .get_mut(&connection)?
            .windows
            .get_mut(&(transfer, index))?;
        match &mut window.hold {
            Hold::Allocated { extent } => Some(extent.bytes_mut()),
            Hold::Lease(_) | Hold::None => None,
        }
    }

    /// Removes a window allocated from a region and hands its extent to the caller
    /// (for example to keep bytes alive until a staging write completes). Leases are
    /// released and yield `None`.
    pub fn take_allocated(&mut self, connection: u64, transfer: u64, index: u32) -> Option<Extent> {
        let window = self
            .connections
            .get_mut(&connection)?
            .windows
            .remove(&(transfer, index))?;
        let mut window = window;
        match window.take_hold() {
            Hold::Allocated { extent } => Some(extent),
            Hold::Lease(extent) => {
                extent.release();
                None
            }
            Hold::None => None,
        }
    }

    pub fn stats(&self, connection: u64) -> Option<ConnectionStats> {
        let record = self.connections.get(&connection)?;
        let bytes = record.windows.values().map(|w| w.length as u64).sum();
        Some(ConnectionStats {
            bytes,
            put_windows: record
                .windows
                .values()
                .filter(|w| w.direction == Direction::Put)
                .count() as u32,
            get_windows: record
                .windows
                .values()
                .filter(|w| w.direction == Direction::Get)
                .count() as u32,
            quarantine_bytes: if record.closed { bytes } else { 0 },
        })
    }

    /// Bytes held by all live and quarantined windows.
    pub fn held_bytes(&self) -> u64 {
        self.connections
            .values()
            .flat_map(|record| record.windows.values())
            .map(|window| window.length as u64)
            .sum()
    }

    /// Bytes held only by closed, not-yet-confirmed-exited connections.
    pub fn quarantine_bytes(&self) -> u64 {
        self.connections
            .values()
            .filter(|record| record.closed)
            .flat_map(|record| record.windows.values())
            .map(|window| window.length as u64)
            .sum()
    }

    /// Whether `bytes` can be allocated without reclaiming anything.
    pub fn can_admit(&self, bytes: u64) -> bool {
        self.can_allocate(Region::Arena, bytes) || self.can_allocate(Region::Staging, bytes)
    }

    pub fn can_allocate(&self, region: Region, bytes: u64) -> bool {
        let Ok(bytes) = usize::try_from(bytes) else {
            return false;
        };
        match region {
            Region::Arena => self.arena.can_allocate(bytes),
            Region::Staging => self.staging.can_allocate(bytes),
        }
    }

    /// Remembers the largest reservation refused `BUDGET` on this connection.
    /// Replaces the armed reservation. The store arms the most recent refusal
    /// for the connection, so a smaller later requirement must replace an
    /// older, unserviceable one rather than being ignored.
    pub fn arm_budget(&mut self, connection: u64, bytes: u64) {
        if let Some(record) = self.connections.get_mut(&connection) {
            record.armed = bytes;
        }
    }

    pub fn armed_budget(&self, connection: u64) -> Option<u64> {
        self.connections
            .get(&connection)
            .map(|record| record.armed)
            .filter(|bytes| *bytes != 0)
    }

    /// Clears and returns the armed reservation without checking capacity. The
    /// store uses this when it has already accounted for eviction and region
    /// requirements itself.
    pub fn take_armed(&mut self, connection: u64) -> Option<u64> {
        let record = self.connections.get_mut(&connection)?;
        let bytes = record.armed;
        record.armed = 0;
        (bytes != 0).then_some(bytes)
    }

    /// Returns the armed reservation once capacity can admit it, clearing the arming.
    /// The caller emits `Room`; this never allocates.
    pub fn take_room(&mut self, connection: u64) -> Option<u64> {
        let bytes = self.armed_budget(connection)?;
        if !self.can_admit(bytes) {
            return None;
        }
        if let Some(record) = self.connections.get_mut(&connection) {
            record.armed = 0;
        }
        Some(bytes)
    }

    fn pid(&self, connection: u64) -> std::result::Result<Option<u32>, GrantError> {
        self.connections
            .get(&connection)
            .map(|record| record.pid)
            .ok_or_else(|| GrantError::Invalid(format!("unknown connection {connection}")))
    }

    fn pid_text(pid: Option<u32>) -> String {
        pid.map_or_else(
            || "peer session".to_owned(),
            |pid| format!("peer pid {pid}"),
        )
    }

    fn check_credit(
        &self,
        connection: u64,
        direction: Direction,
    ) -> std::result::Result<(), GrantError> {
        let record = self
            .connections
            .get(&connection)
            .ok_or_else(|| GrantError::Invalid(format!("unknown connection {connection}")))?;
        let granted = record
            .windows
            .values()
            .filter(|window| window.direction == direction)
            .count();
        if granted >= WINDOW_CREDITS as usize {
            Err(GrantError::Credits)
        } else {
            Ok(())
        }
    }

    fn check_new_window(
        &self,
        connection: u64,
        transfer: u64,
        index: u32,
        pid: &str,
    ) -> std::result::Result<(), GrantError> {
        if self.connections[&connection]
            .windows
            .contains_key(&(transfer, index))
        {
            return Err(GrantError::Invalid(format!(
                "duplicate window {transfer}/{index} (peer pid {pid})"
            )));
        }
        Ok(())
    }
}

/// Creates `directory` with mode 0770 when it is missing. An existing directory
/// keeps its mode and ownership: the manager only sets a mode on a directory it
/// created. A directory this process cannot write is reported with its path and
/// owner instead of a bare `EPERM`, which usually only says the caller is not
/// the owner.
fn create_private_directory(directory: &Path) -> Result<()> {
    match fs::create_dir(directory) {
        Ok(()) => return fs::set_permissions(directory, fs::Permissions::from_mode(0o770)),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error),
    }
    if directory_writable(directory) {
        return Ok(());
    }
    let owner = fs::metadata(directory)
        .map(|metadata| {
            use std::os::unix::fs::MetadataExt;
            format!(" (owned by uid {} gid {})", metadata.uid(), metadata.gid())
        })
        .unwrap_or_default();
    Err(invalid(format!(
        "{}: existing directory is not writable by this process{owner}",
        directory.display()
    )))
}

/// Whether this process may create files in `directory`, judged by its
/// effective uid and gid.
fn directory_writable(directory: &Path) -> bool {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};
    let Ok(path) = CString::new(directory.as_os_str().as_bytes()) else {
        return false;
    };
    unsafe { libc::faccessat(libc::AT_FDCWD, path.as_ptr(), libc::W_OK, libc::AT_EACCESS) == 0 }
}

fn validate_instance(instance: &str) -> Result<()> {
    if instance.is_empty()
        || instance == "."
        || instance == ".."
        || instance.contains('/')
        || instance.contains('\0')
    {
        return Err(invalid(format!("invalid instance name '{instance}'")));
    }
    Ok(())
}

fn boot_nonce() -> Result<String> {
    let mut bytes = [0u8; 8];
    File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    let mut nonce = String::with_capacity(16);
    for byte in bytes {
        use std::fmt::Write;
        write!(nonce, "{byte:02x}").unwrap();
    }
    Ok(nonce)
}

/// Removes every stale entry a crashed owner left in the instance directory.
///
/// Ownership is the held flock, never the directory's existence: by the time
/// this runs the lock is held, so anything except the lock file itself is
/// debris from a manager that died without cleaning up. Directories are left
/// alone; this instance's directory only ever holds the lock and region files.
fn reclaim_stale(directory: &Path) -> Result<usize> {
    let mut removed = 0;
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if entry.file_name() == "lock" || entry.file_type()?.is_dir() {
            continue;
        }
        match fs::remove_file(entry.path()) {
            Ok(()) => removed += 1,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(removed)
}

/// Creates a fresh region file, sets 0660 and `fallocate`s it to its full size.
fn create_region(
    directory: &Path,
    prefix: &str,
    nonce: &str,
    bytes: usize,
) -> Result<(PathBuf, usize)> {
    let size = bytes / PAGE_BYTES as usize * PAGE_BYTES as usize;
    if size == 0 {
        return Err(invalid(format!(
            "{prefix} region must hold at least {PAGE_BYTES} bytes"
        )));
    }
    let path = directory.join(format!("{prefix}-{nonce}"));
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .mode(0o660)
        .open(&path)?;
    if unsafe { libc::fchmod(file.as_raw_fd(), 0o660) } != 0 {
        return Err(io::Error::last_os_error());
    }
    if unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, size as libc::off_t) } != 0 {
        let error = io::Error::last_os_error();
        let _ = fs::remove_file(&path);
        return Err(error);
    }
    Ok((path, size))
}

enum Pidfd {
    Open(OwnedFd),
    Exited,
    Unknown,
}

fn open_pidfd(pid: u32) -> Pidfd {
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid as libc::pid_t, 0u32) } as i32;
    if fd >= 0 {
        return Pidfd::Open(unsafe { OwnedFd::from_raw_fd(fd) });
    }
    match io::Error::last_os_error().raw_os_error() {
        Some(libc::ESRCH) => Pidfd::Exited,
        // EPERM and everything else is not proof of exit: keep quarantining.
        _ => Pidfd::Unknown,
    }
}

fn pidfd_exited(pidfd: &OwnedFd) -> bool {
    let mut poll = libc::pollfd {
        fd: pidfd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    let result = unsafe { libc::poll(&mut poll, 1, 0) };
    result > 0 && poll.revents != 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        os::unix::fs::PermissionsExt,
        process::{Child, Command},
        sync::atomic::{AtomicU64, Ordering},
        thread,
        time::{Duration, Instant},
    };

    static NEXT: AtomicU64 = AtomicU64::new(0);

    fn instance(tag: &str) -> String {
        format!(
            "selene-local-{}-{tag}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )
    }

    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn local(name: &str, arena: usize, staging: usize) -> (Local, Cleanup) {
        let local = Local::open(name, arena, staging, 4 * 1024 * 1024).unwrap();
        let cleanup = Cleanup(local.directory().to_path_buf());
        (local, cleanup)
    }

    /// Reopening an instance right after its owner was dropped can transiently observe the
    /// lock still held by a child this process spawned for another test: the child inherits
    /// the lock descriptor and keeps the flock until its exec closes it (CLOEXEC). Retry that
    /// window instead of assuming close and release are the same instant.
    fn reopen(name: &str) -> Local {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match Local::open(name, 64 * 1024, 64 * 1024, 4 * 1024 * 1024) {
                Ok(local) => return local,
                Err(error)
                    if error.kind() == io::ErrorKind::WouldBlock && Instant::now() < deadline =>
                {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("reopening instance '{name}' failed: {error}"),
            }
        }
    }

    fn shape(pages: u32, row: bool) -> WindowShape {
        WindowShape::new(pages, row, 4096, if row { 4096 } else { 0 }).unwrap()
    }

    struct ChildGuard(Child);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    fn spawn_sleeper() -> ChildGuard {
        ChildGuard(
            Command::new("/bin/sh")
                .args(["-c", "sleep 30"])
                .spawn()
                .unwrap(),
        )
    }

    fn wait_report(local: &mut Local, connection: u64) -> Vec<u64> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let mut exited = Vec::new();
            local.poll_exits(&mut exited);
            if exited.contains(&connection) {
                return exited;
            }
            assert!(
                Instant::now() < deadline,
                "pidfd never reported connection {connection}"
            );
            thread::sleep(Duration::from_millis(2));
        }
    }

    #[test]
    fn the_same_instance_cannot_be_opened_twice() {
        let name = instance("lock");
        let (first, _cleanup) = local(&name, 64 * 1024, 64 * 1024);
        let error = Local::open(&name, 64 * 1024, 64 * 1024, 4 * 1024 * 1024)
            .err()
            .unwrap();
        assert!(
            error.to_string().contains("already in use"),
            "unexpected error: {error}"
        );
        drop(first);
        let second = reopen(&name);
        drop(second);
    }

    #[test]
    fn stale_regions_are_unlinked_and_boot_nonce_is_fresh() {
        let name = instance("stale");
        let directory;
        let old_nonce;
        {
            let first = Local::open(&name, 64 * 1024, 64 * 1024, 4 * 1024 * 1024).unwrap();
            directory = first.directory().to_path_buf();
            old_nonce = first.boot_nonce().to_owned();
            assert_eq!(old_nonce.len(), 16);
            assert!(
                old_nonce
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            );
            assert!(first.arena_path().exists());
            assert!(first.staging_path().exists());
        }
        fs::write(directory.join("arena-0123456789abcdef"), b"stale").unwrap();
        fs::write(directory.join("staging-0123456789abcdef"), b"stale").unwrap();
        fs::write(directory.join("leftover.tmp"), b"stale").unwrap();
        let second = reopen(&name);
        let _cleanup = Cleanup(directory.clone());
        assert!(!directory.join("arena-0123456789abcdef").exists());
        assert!(!directory.join("staging-0123456789abcdef").exists());
        assert!(!directory.join(format!("arena-{old_nonce}")).exists());
        assert!(!directory.join("leftover.tmp").exists());
        assert_ne!(second.boot_nonce(), old_nonce);
        assert!(directory.join("lock").exists());
        assert!(second.arena_path().exists());
        assert_eq!(
            fs::metadata(second.arena_path()).unwrap().len(),
            second.arena_size()
        );
        assert_eq!(
            fs::metadata(second.staging_path()).unwrap().len(),
            second.staging_size()
        );
        let mode = fs::metadata(second.arena_path())
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o660);
    }

    #[test]
    fn an_existing_instance_directory_keeps_its_mode() {
        let name = instance("mode");
        let directory = Path::new(SHM_ROOT).join(&name);
        fs::create_dir_all(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o755)).unwrap();
        let local = Local::open(&name, 64 * 1024, 64 * 1024, 4 * 1024 * 1024).unwrap();
        let _cleanup = Cleanup(directory.clone());
        let mode = fs::metadata(local.directory())
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o755, "an existing directory keeps its mode");
    }

    #[test]
    fn an_unwritable_instance_directory_names_itself() {
        if unsafe { libc::geteuid() } == 0 {
            // Root may write any directory, so this failure cannot be staged.
            return;
        }
        let name = instance("unwritable");
        let directory = Path::new(SHM_ROOT).join(&name);
        fs::create_dir_all(&directory).unwrap();
        let _cleanup = Cleanup(directory.clone());
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o500)).unwrap();
        let error = Local::open(&name, 64 * 1024, 64 * 1024, 4 * 1024 * 1024)
            .err()
            .unwrap()
            .to_string();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        assert!(
            error.contains(&directory.display().to_string()) && error.contains("uid"),
            "{error}"
        );
    }

    #[test]
    fn window_shape_is_whole_units_within_the_negotiated_maximum() {
        assert!(WindowShape::new(64, false, 4096, 0).is_ok());
        assert!(WindowShape::new(65, false, 4096, 0).is_err());
        assert!(WindowShape::new(0, false, 0, 0).is_err());
        assert!(WindowShape::new(1, false, 4097, 0).is_err());
        assert!(WindowShape::new(1, true, 4096, 4097).is_err());
        assert!(WindowShape::new(0, true, 0, 0).is_err());

        let page = WindowShape::new(1, false, 4096, 0).unwrap();
        assert!(page.fits(4096));
        assert!(!WindowShape::new(2, false, 4096, 0).unwrap().fits(4096));

        let oversized_page = WindowShape::new(1, false, 8 * 1024 * 1024, 0).unwrap();
        assert_eq!(oversized_page.length(), 8 * 1024 * 1024);
        assert!(oversized_page.fits(4 * 1024 * 1024));
        assert!(oversized_page.validate(4 * 1024 * 1024).is_ok());

        let oversized_row = WindowShape::new(0, true, 0, 8 * 1024 * 1024).unwrap();
        assert!(oversized_row.fits(4 * 1024 * 1024));
        let row_and_page = WindowShape::new(1, true, 4 * 1024 * 1024, 4 * 1024 * 1024).unwrap();
        assert!(!row_and_page.fits(4 * 1024 * 1024));
        assert!(row_and_page.validate(4 * 1024 * 1024).is_err());
    }

    #[test]
    fn ranges_are_bounds_checked_with_the_peer_pid() {
        assert!(check_range(8192, 4096, 4096, 7).is_ok());
        let error = check_range(8192, 1, 4096, 7).unwrap_err();
        assert!(error.to_string().contains("peer pid 7"));
        assert!(check_range(8192, 4096, 1, 7).is_err());
        let error = check_range(8192, 8192, 4096, 7).unwrap_err();
        assert!(error.to_string().contains("peer pid 7"));
        assert!(check_range(8192, u64::MAX, 4096, 7).is_err());
        assert!(check_range(8192, 0, 0, 7).is_ok());
    }

    #[test]
    fn grants_reject_partial_units_and_accept_an_oversized_single_unit() {
        let name = instance("shape");
        let (mut local, _cleanup) = local(&name, 16 * 1024 * 1024, 16 * 1024 * 1024);
        local.accept(1, std::process::id()).unwrap();

        let partial = WindowShape::new(1, false, 4096 + 512, 0);
        assert!(partial.is_err());

        let grant = local
            .grant_put(1, 1, 0, Region::Arena, shape(1, false))
            .unwrap();
        assert_eq!(grant.length, 4096);
        assert_eq!(grant.offset % PAGE_BYTES, 0);

        let oversized = WindowShape::new(0, true, 0, 8 * 1024 * 1024).unwrap();
        let grant = local.grant_put(1, 1, 1, Region::Arena, oversized).unwrap();
        assert_eq!(grant.length, 8 * 1024 * 1024);
        assert!(grant.row);
        assert_eq!(grant.pages, 0);
    }

    #[test]
    fn credits_are_two_per_direction_and_release_recycles_them() {
        let name = instance("credits");
        let (mut local, _cleanup) = local(&name, 64 * 1024, 64 * 1024);
        local.accept(1, std::process::id()).unwrap();

        local
            .grant_put(1, 1, 0, Region::Arena, shape(1, false))
            .unwrap();
        local
            .grant_put(1, 1, 1, Region::Arena, shape(1, false))
            .unwrap();
        assert_eq!(
            local.grant_put(1, 1, 2, Region::Arena, shape(1, false)),
            Err(GrantError::Credits)
        );
        // Put credits do not constrain Get credits.
        local.grant_get_staging(1, 2, 0, shape(1, false)).unwrap();
        assert!(local.release(1, 1, 0).is_ok());
        local
            .grant_put(1, 1, 2, Region::Arena, shape(1, false))
            .unwrap();
        let stats = local.stats(1).unwrap();
        assert_eq!(stats.put_windows, 2);
        assert_eq!(stats.get_windows, 1);
        assert_eq!(local.release(1, 1, 0), Err(ReleaseError::UnknownWindow));
    }

    #[test]
    fn a_full_region_returns_budget_and_never_blocks() {
        let name = instance("budget");
        let (mut local, _cleanup) = local(&name, 4096, 4096);
        local.accept(1, std::process::id()).unwrap();
        local
            .grant_put(1, 1, 0, Region::Arena, shape(1, false))
            .unwrap();
        assert_eq!(
            local.grant_put(1, 1, 1, Region::Arena, shape(1, false)),
            Err(GrantError::Budget)
        );
        // Staging is a different region; Get has its own credits.
        local.grant_get_staging(1, 2, 0, shape(1, false)).unwrap();
        assert_eq!(
            local.grant_get_staging(1, 2, 1, shape(1, false)),
            Err(GrantError::Budget)
        );
        local.release(1, 1, 0).unwrap();
        // The released arena window is reusable.
        local
            .grant_put(1, 1, 0, Region::Arena, shape(1, false))
            .unwrap();
    }

    #[test]
    fn leases_keep_the_extent_alive_until_released() {
        let name = instance("lease");
        let (mut local, _cleanup) = local(&name, 8192, 8192);
        local.accept(1, std::process::id()).unwrap();
        let arena = local.arena();
        let extent = Arc::new(arena.allocate(4096).unwrap());
        assert_eq!(arena.used(), 4096);

        let grant = local
            .grant_get_lease(1, 1, 0, shape(1, false), &extent, 0)
            .unwrap();
        assert_eq!(grant.offset % PAGE_BYTES, 0);
        assert_eq!(grant.length, 4096);
        drop(extent);
        assert_eq!(arena.used(), 4096, "the lease holds the allocation");
        assert_eq!(local.held_bytes(), 4096);

        local.release(1, 1, 0).unwrap();
        assert_eq!(arena.used(), 0);
        assert_eq!(local.held_bytes(), 0);
    }

    #[test]
    fn a_live_disconnected_child_keeps_its_window_quarantined() {
        let mut child = spawn_sleeper();
        let pid = child.0.id();
        let name = instance("quarantine");
        let (mut local, _cleanup) = local(&name, 4096, 4096);
        local.accept(7, pid).unwrap();
        local
            .grant_put(7, 1, 0, Region::Arena, shape(1, false))
            .unwrap();
        local.close(7);

        let mut exited = Vec::new();
        local.poll_exits(&mut exited);
        assert!(exited.is_empty());
        let stats = local.stats(7).unwrap();
        assert_eq!(stats.bytes, 4096);
        assert_eq!(stats.quarantine_bytes, 4096);
        assert_eq!(local.held_bytes(), 4096);
        assert_eq!(local.quarantine_bytes(), 4096);
        assert_eq!(local.arena().used(), 4096);

        // No room has been freed: a second connection's reuse attempt is refused.
        local.accept(8, std::process::id()).unwrap();
        assert_eq!(
            local.grant_put(8, 2, 0, Region::Arena, shape(1, false)),
            Err(GrantError::Budget)
        );

        child.0.kill().unwrap();
        child.0.wait().unwrap();
        let exited = wait_report(&mut local, 7);
        assert_eq!(exited, vec![7]);
        assert_eq!(local.held_bytes(), 0);
        assert_eq!(local.quarantine_bytes(), 0);
        assert_eq!(local.arena().used(), 0);
        assert!(local.stats(7).is_none());
        local
            .grant_put(8, 2, 0, Region::Arena, shape(1, false))
            .unwrap();
    }

    #[test]
    fn exit_alone_does_not_reclaim_an_open_connection() {
        let mut child = spawn_sleeper();
        let pid = child.0.id();
        let name = instance("exit-open");
        let (mut local, _cleanup) = local(&name, 4096, 4096);
        local.accept(9, pid).unwrap();
        local
            .grant_put(9, 3, 0, Region::Arena, shape(1, false))
            .unwrap();

        child.0.kill().unwrap();
        child.0.wait().unwrap();
        wait_report(&mut local, 9);
        assert_eq!(local.held_bytes(), 4096, "connection is still open");
        local.close(9);
        assert_eq!(local.held_bytes(), 0);
        assert!(local.stats(9).is_none());
    }

    #[test]
    fn independent_instances_do_not_share_bytes_or_offsets() {
        let left_name = instance("left");
        let right_name = instance("right");
        let (mut left, _left_cleanup) = local(&left_name, 64 * 1024, 64 * 1024);
        let (mut right, _right_cleanup) = local(&right_name, 64 * 1024, 64 * 1024);
        left.accept(1, std::process::id()).unwrap();
        right.accept(1, std::process::id()).unwrap();
        let a = left
            .grant_put(1, 1, 0, Region::Arena, shape(1, false))
            .unwrap();
        let b = right
            .grant_put(1, 1, 0, Region::Arena, shape(1, false))
            .unwrap();
        assert_ne!(left.arena_path(), right.arena_path());
        assert_ne!(left.boot_nonce(), right.boot_nonce());

        left.window_bytes_mut(1, 1, 0).unwrap()[..4].copy_from_slice(b"AAAA");
        right.window_bytes_mut(1, 1, 0).unwrap()[..4].copy_from_slice(b"BBBB");
        assert_eq!(&left.window_bytes(1, 1, 0).unwrap()[..4], b"AAAA");
        assert_eq!(&right.window_bytes(1, 1, 0).unwrap()[..4], b"BBBB");
        assert_eq!(a.length, b.length);
    }

    #[test]
    fn remote_sessions_grant_without_a_local_pid() {
        let name = instance("remote");
        let (mut local, _cleanup) = local(&name, 8192, 8192);
        local.accept_remote(1).unwrap();
        local
            .grant_put(1, 1, 0, Region::Arena, shape(1, false))
            .unwrap();
        assert_eq!(local.stats(1).unwrap().bytes, 4096);
        local.close(1);
        assert_eq!(local.held_bytes(), 0);
        assert_eq!(local.arena().used(), 0);
        assert!(local.stats(1).is_none());
    }

    #[test]
    fn region_mapping_covers_the_region_and_writes_are_visible() {
        let name = instance("mapping");
        let (mut local, _cleanup) = local(&name, 64 * 1024, 64 * 1024);
        local.accept(1, std::process::id()).unwrap();
        let grant = local
            .grant_put(1, 1, 0, Region::Staging, shape(1, false))
            .unwrap();
        let (pointer, length) = local.region_mapping(Region::Staging);
        assert_eq!(length as u64, local.staging_size());
        let (arena, arena_length) = local.region_mapping(Region::Arena);
        assert_eq!(arena_length as u64, local.arena_size());
        assert!(!arena.is_null() && !pointer.is_null());
        unsafe {
            std::ptr::copy_nonoverlapping(b"map!".as_ptr(), pointer.add(grant.offset as usize), 4);
        }
        assert_eq!(&local.window_bytes(1, 1, 0).unwrap()[..4], b"map!");
    }

    #[test]
    fn armed_budget_turns_into_room_once_capacity_frees() {
        let name = instance("room");
        let (mut local, _cleanup) = local(&name, 8192, 4096);
        local.accept(1, std::process::id()).unwrap();
        local
            .grant_put(1, 1, 0, Region::Arena, shape(1, false))
            .unwrap();
        local
            .grant_put(1, 1, 1, Region::Arena, shape(1, false))
            .unwrap();
        // Fill staging too so no region can admit the armed reservation.
        local.grant_get_staging(1, 2, 0, shape(1, false)).unwrap();
        assert_eq!(local.armed_budget(1), None);
        local.arm_budget(1, 4096);
        assert_eq!(local.armed_budget(1), Some(4096));
        assert_eq!(local.take_room(1), None);
        local.release(1, 1, 0).unwrap();
        assert_eq!(local.take_room(1), Some(4096));
        assert_eq!(local.armed_budget(1), None);
    }
}
