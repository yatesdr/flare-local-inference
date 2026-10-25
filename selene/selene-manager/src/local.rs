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
pub const MAX_WINDOW_PAGES: u32 = flare_selene::slab::WINDOW_PAGES as u32;
/// Granted windows per direction per connection.
pub use flare_selene::pool::WINDOW_CREDITS;

pub use crate::data::Region;

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
#[cfg(test)]
fn check_range(
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

struct Window {
    direction: Direction,
    length: usize,
    #[cfg(test)]
    within: usize,
    hold: Arc<Extent>,
}

impl Drop for Window {
    fn drop(&mut self) {
        self.hold.release();
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
}

/// Who owns a session's windows. Sessions of instances on other hosts have no local pidfd and
/// are leased no windows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionKind {
    Local { pid: u32 },
    Remote,
}

/// One manager instance's shared-memory regions and the windows granted from them.
pub struct Local {
    instance: String,
    directory: PathBuf,
    _lock: File,
    arena: Arena,
    staging: Arena,
    arena_path: PathBuf,
    staging_path: PathBuf,
    arena_size: u64,
    staging_size: u64,
    max_window_bytes: u64,
    connections: HashMap<u64, Connection>,
    /// Identifies this set of region files. It changes exactly when the files are recreated, so
    /// a process that registered the old files can tell the new ones are different.
    regions_generation: u64,
    /// Processes of the previous run that may still access the reused regions.
    prior: Option<Prior>,
    /// True while `prior` holds the regions; the pool reads it to refuse claims distinctly.
    held: Arc<std::sync::atomic::AtomicBool>,
}

/// The reused regions' previous run: nothing in them is granted until every process that held
/// a session then has reconnected (so it has drained) or exited. See PROTOCOL.md section 8.2.
struct Prior {
    /// Pid to a descriptor that reports its exit; `None` when the process cannot be watched,
    /// which holds until it reconnects.
    holders: HashMap<u32, Option<OwnedFd>>,
    /// The whole of both regions, so no grant or store allocation can land in them.
    _extents: Vec<Extent>,
}

/// File names inside an instance directory. They are fixed so that a restart finds the files it
/// left; everything else in the directory is debris.
const ARENA_FILE: &str = "arena";
const STAGING_FILE: &str = "staging";
const GENERATION_FILE: &str = "generation";
const HOLDERS_FILE: &str = "holders";
const LOCK_FILE: &str = "lock";

/// Names the startup step and path an error came from.
fn step_error(step: &str, path: &Path, error: io::Error) -> io::Error {
    io::Error::new(error.kind(), format!("{step} {}: {error}", path.display()))
}

impl Local {
    /// Owns `/dev/shm/selene/<instance>`: takes the exclusive lock, reclaims the debris a
    /// crashed owner left behind and reuses its arena and staging files when they have the
    /// configured sizes (same files, inodes and offsets), else creates fresh, preallocated
    /// ones under a new generation.
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

        let lock_path = directory.join(LOCK_FILE);
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

        let arena_size = region_size_of(ARENA_FILE, arena_bytes)?;
        let staging_size = region_size_of(STAGING_FILE, staging_bytes)?;
        let arena_path = directory.join(ARENA_FILE);
        let staging_path = directory.join(STAGING_FILE);
        let reused = match reusable(&directory, arena_size, staging_size) {
            Ok(generation) => Some(generation),
            Err(reason) => {
                let existed = arena_path.exists() || staging_path.exists();
                for name in [ARENA_FILE, STAGING_FILE, GENERATION_FILE, HOLDERS_FILE] {
                    let _ = fs::remove_file(directory.join(name));
                }
                if existed {
                    eprintln!(
                        "selene: instance '{instance}' recreated its shared regions ({reason}); \
                         running instances must restart to use the pool"
                    );
                }
                None
            }
        };
        let regions_generation = match reused {
            Some(generation) => generation,
            None => {
                create_region(&directory, ARENA_FILE, arena_size)
                    .map_err(|error| step_error("create the arena in", &directory, error))?;
                create_region(&directory, STAGING_FILE, staging_size)
                    .map_err(|error| step_error("create staging in", &directory, error))?;
                let generation = new_generation()?;
                write_atomically(
                    &directory.join(GENERATION_FILE),
                    format!("{generation:016x}\n").as_bytes(),
                )
                .map_err(|error| step_error("write the generation in", &directory, error))?;
                generation
            }
        };
        let arena = Arena::open(&arena_path, arena_size)?;
        let staging = Arena::open(&staging_path, staging_size)?;
        if reused.is_some() {
            eprintln!(
                "selene: instance '{instance}' reused its shared regions (generation \
                 {regions_generation:016x})"
            );
        }
        let prior = if reused.is_some() {
            Prior::read(&directory, &arena, &staging, instance)
        } else {
            None
        };

        let local = Self {
            instance: instance.to_owned(),
            directory,
            _lock: lock,
            arena,
            staging,
            arena_path,
            staging_path,
            arena_size: arena_size as u64,
            staging_size: staging_size as u64,
            max_window_bytes,
            connections: HashMap::new(),
            regions_generation,
            held: Arc::new(std::sync::atomic::AtomicBool::new(prior.is_some())),
            prior,
        };
        local.persist_holders();
        Ok(local)
    }

    /// Identifies the region files. Equal values mean the same files at the same offsets; a
    /// process that mapped and registered them can keep using them.
    pub fn regions_generation(&self) -> u64 {
        self.regions_generation
    }

    /// The flag that is true while the regions are held for the previous run.
    pub fn held_flag(&self) -> Arc<std::sync::atomic::AtomicBool> {
        self.held.clone()
    }

    /// Whether the regions are still held for processes of the previous run.
    pub fn held_for_previous_run(&self) -> bool {
        self.prior.is_some()
    }

    /// Writes the pids with a live local session, and the previous run's still-unresolved
    /// holders, so a restart after a crash knows whose copies could still be in flight.
    fn persist_holders(&self) {
        let mut pids: Vec<u32> = self
            .connections
            .values()
            .filter(|record| !record.exited)
            .filter_map(|record| record.pid)
            .collect();
        if let Some(prior) = &self.prior {
            pids.extend(prior.holders.keys());
        }
        pids.sort_unstable();
        pids.dedup();
        let mut text = String::new();
        for pid in pids {
            if let Some(start) = process_start(pid) {
                text.push_str(&format!("{pid} {start}\n"));
            }
        }
        let _ = write_atomically(&self.directory.join(HOLDERS_FILE), text.as_bytes());
    }

    /// Releases the previous run's hold once none of its processes can still be copying.
    fn settle_prior(&mut self) {
        if self
            .prior
            .as_ref()
            .is_some_and(|prior| prior.holders.is_empty())
        {
            self.prior = None;
            self.held.store(false, std::sync::atomic::Ordering::Release);
            eprintln!(
                "selene: instance '{}' released the shared regions held for the previous run",
                self.instance
            );
        }
    }

    pub fn instance(&self) -> &str {
        &self.instance
    }
    pub fn directory(&self) -> &Path {
        &self.directory
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
    pub fn arena(&self) -> Arena {
        self.arena.clone()
    }
    pub fn staging(&self) -> Arena {
        self.staging.clone()
    }
    fn region_size(&self, region: Region) -> u64 {
        match region {
            Region::Arena => self.arena_size,
            Region::Staging => self.staging_size,
        }
    }
    /// Base pointer and full mapped length of a region. Valid for the life of the
    /// mapping and stable across allocations.
    #[cfg(test)]
    fn region_mapping(&self, region: Region) -> (*mut u8, usize) {
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

    /// Registers a session of an instance on another host. It has no local pidfd and is leased
    /// no windows; `close` removes it.
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
                let (pidfd, exited) = watch_pid(pid);
                (Some(pid), pidfd, exited)
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
            },
        );
        // A process of the previous run that reconnects has drained what it did under the
        // dead manager before it did (PROTOCOL.md section 8.1), so its hold is over.
        if let (SessionKind::Local { pid }, Some(prior)) = (kind, &mut self.prior) {
            prior.holders.remove(&pid);
            self.settle_prior();
        }
        self.persist_holders();
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
        self.persist_holders();
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
        if let Some(prior) = &mut self.prior {
            let before = prior.holders.len();
            prior
                .holders
                .retain(|_, pidfd| !pidfd.as_ref().is_some_and(pidfd_exited));
            if prior.holders.len() != before {
                self.settle_prior();
                self.persist_holders();
            }
        }
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
                    #[cfg(test)]
                    within,
                    hold: extent.clone(),
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

    /// Releases every window of one transfer, whatever its state. Used when the party that
    /// may still touch them has proven it stopped.
    pub fn release_transfer(&mut self, connection: u64, transfer: u64) -> usize {
        let Some(record) = self.connections.get_mut(&connection) else {
            return 0;
        };
        let before = record.windows.len();
        record.windows.retain(|(id, _), _| *id != transfer);
        before - record.windows.len()
    }

    #[cfg(test)]
    fn window_bytes(&self, connection: u64, transfer: u64, index: u32) -> Option<&[u8]> {
        let window = self
            .connections
            .get(&connection)?
            .windows
            .get(&(transfer, index))?;
        Some(&window.hold.bytes()[window.within..window.within + window.length])
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
    #[cfg(test)]
    fn held_bytes(&self) -> u64 {
        self.connections
            .values()
            .flat_map(|record| record.windows.values())
            .map(|window| window.length as u64)
            .sum()
    }

    /// Bytes held only by closed, not-yet-confirmed-exited connections.
    #[cfg(test)]
    fn quarantine_bytes(&self) -> u64 {
        self.connections
            .values()
            .filter(|record| record.closed)
            .flat_map(|record| record.windows.values())
            .map(|window| window.length as u64)
            .sum()
    }

    #[cfg(test)]
    fn can_allocate(&self, region: Region, bytes: u64) -> bool {
        let Ok(bytes) = usize::try_from(bytes) else {
            return false;
        };
        match region {
            Region::Arena => self.arena.can_allocate(bytes),
            Region::Staging => self.staging.can_allocate(bytes),
        }
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


/// Removes every stale entry a crashed owner left in the instance directory.
///
/// Ownership is the held flock, never the directory's existence: by the time
/// this runs the lock is held, so no other manager of this instance is running. Anything but
/// the lock and the files a restart reuses (the two regions, their generation and the holders
/// list) is debris from a manager that died without cleaning up. Only this instance's own
/// directory is read, so another instance's files are never touched. Directories are left
/// alone; this instance's directory only ever holds the lock and region files.
fn reclaim_stale(directory: &Path) -> Result<usize> {
    let mut removed = 0;
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry.file_name();
        let kept = [
            LOCK_FILE,
            ARENA_FILE,
            STAGING_FILE,
            GENERATION_FILE,
            HOLDERS_FILE,
        ];
        if kept.iter().any(|kept| name == *kept) || entry.file_type()?.is_dir() {
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

/// A region's size: the configured bytes rounded down to a page.
fn region_size_of(name: &str, bytes: usize) -> Result<usize> {
    let size = bytes / PAGE_BYTES as usize * PAGE_BYTES as usize;
    if size == 0 {
        return Err(invalid(format!(
            "{name} region must hold at least {PAGE_BYTES} bytes"
        )));
    }
    Ok(size)
}

/// Creates a fresh region file, sets 0660 and `fallocate`s it to its full size.
fn create_region(directory: &Path, name: &str, size: usize) -> Result<PathBuf> {
    let path = directory.join(name);
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
    Ok(path)
}

/// The generation of the region files a previous run left, if they can be used as they are:
/// regular files of this user, exactly the configured sizes, with their memory reserved, and a
/// readable generation. Otherwise why not.
fn reusable(
    directory: &Path,
    arena_size: usize,
    staging_size: usize,
) -> std::result::Result<u64, String> {
    use std::os::unix::fs::MetadataExt;
    for (name, size) in [(ARENA_FILE, arena_size), (STAGING_FILE, staging_size)] {
        let path = directory.join(name);
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(format!("no {name} file"));
            }
            Err(error) => return Err(format!("{name}: {error}")),
        };
        if !metadata.is_file() {
            return Err(format!("{name} is not a regular file"));
        }
        if metadata.uid() != unsafe { libc::geteuid() } {
            return Err(format!("{name} belongs to another user"));
        }
        if metadata.len() != size as u64 {
            return Err(format!(
                "{name} is {} bytes and {size} are configured",
                metadata.len()
            ));
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&path)
            .map_err(|error| format!("{name}: {error}"))?;
        if unsafe { libc::fchmod(file.as_raw_fd(), 0o660) } != 0 {
            return Err(format!("{name}: {}", io::Error::last_os_error()));
        }
        if unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, size as libc::off_t) } != 0 {
            return Err(format!("{name}: {}", io::Error::last_os_error()));
        }
    }
    let text = fs::read_to_string(directory.join(GENERATION_FILE))
        .map_err(|error| format!("generation: {error}"))?;
    match u64::from_str_radix(text.trim(), 16) {
        Ok(generation) if generation != 0 => Ok(generation),
        _ => Err("the generation file is unreadable".to_owned()),
    }
}

/// A random nonzero 64-bit identity for a new set of region files.
fn new_generation() -> Result<u64> {
    loop {
        let mut bytes = [0u8; 8];
        File::open("/dev/urandom")?.read_exact(&mut bytes)?;
        let value = u64::from_le_bytes(bytes);
        if value != 0 {
            return Ok(value);
        }
    }
}

/// Replaces `path` with `contents` in one step, so a crash leaves the old file or the new one.
fn write_atomically(path: &Path, contents: &[u8]) -> io::Result<()> {
    let temporary = path.with_extension("tmp");
    fs::write(&temporary, contents)?;
    fs::set_permissions(&temporary, fs::Permissions::from_mode(0o660))?;
    fs::rename(&temporary, path)
}

/// When a process started, in clock ticks since boot: with its pid it names one process, so a
/// recycled pid is not mistaken for the one that held a session.
fn process_start(pid: u32) -> Option<u64> {
    let text = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // The name may contain spaces and parentheses; everything after the last ')' is plain.
    let rest = &text[text.rfind(')')? + 1..];
    rest.split_whitespace().nth(19)?.parse().ok()
}

impl Prior {
    /// The processes the last run recorded that are still the same live processes. `None` when
    /// there are none, so nothing needs holding.
    fn read(directory: &Path, arena: &Arena, staging: &Arena, instance: &str) -> Option<Self> {
        let text = fs::read_to_string(directory.join(HOLDERS_FILE)).ok()?;
        let mut holders = HashMap::new();
        for line in text.lines() {
            let mut fields = line.split_whitespace();
            let (Some(pid), Some(start)) = (
                fields.next().and_then(|field| field.parse::<u32>().ok()),
                fields.next().and_then(|field| field.parse::<u64>().ok()),
            ) else {
                continue;
            };
            if process_start(pid) != Some(start) {
                continue;
            }
            let (pidfd, exited) = watch_pid(pid);
            if !exited {
                holders.insert(pid, pidfd);
            }
        }
        if holders.is_empty() {
            return None;
        }
        eprintln!(
            "selene: instance '{instance}' holds its shared regions until {} process(es) of the \
             previous run reconnect or exit",
            holders.len()
        );
        let mut extents = arena.reserve_all();
        extents.extend(staging.reserve_all());
        Some(Self {
            holders,
            _extents: extents,
        })
    }
}

/// A missing descriptor is not proof of exit unless pidfd_open returned ESRCH.
pub(crate) fn watch_pid(pid: u32) -> (Option<OwnedFd>, bool) {
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid as libc::pid_t, 0u32) } as i32;
    if fd >= 0 {
        return (Some(unsafe { OwnedFd::from_raw_fd(fd) }), false);
    }
    (
        None,
        io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH),
    )
}

pub(crate) fn pidfd_exited(pidfd: &OwnedFd) -> bool {
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

    impl Local {
        fn grant_fixture(
            &mut self,
            connection: u64,
            transfer: u64,
            index: u32,
            direction: Direction,
            region: Region,
            shape: WindowShape,
        ) -> std::result::Result<WindowGrant, GrantError> {
            shape
                .validate(self.max_window_bytes)
                .map_err(|e| GrantError::Invalid(e.to_string()))?;
            self.check_credit(connection, direction)?;
            let arena = match region {
                Region::Arena => &self.arena,
                Region::Staging => &self.staging,
            };
            let extent = Arc::new(
                arena
                    .allocate(shape.length() as usize)
                    .ok_or(GrantError::Budget)?,
            );
            match direction {
                Direction::Put => {
                    self.grant_put_extent(connection, transfer, index, region, shape, &extent, 0)
                }
                Direction::Get => {
                    self.grant_get_extent(connection, transfer, index, region, shape, &extent, 0)
                }
            }
        }

        fn grant_put(
            &mut self,
            connection: u64,
            transfer: u64,
            index: u32,
            region: Region,
            shape: WindowShape,
        ) -> std::result::Result<WindowGrant, GrantError> {
            self.grant_fixture(connection, transfer, index, Direction::Put, region, shape)
        }

        fn grant_get_staging(
            &mut self,
            connection: u64,
            transfer: u64,
            index: u32,
            shape: WindowShape,
        ) -> std::result::Result<WindowGrant, GrantError> {
            self.grant_fixture(
                connection,
                transfer,
                index,
                Direction::Get,
                Region::Staging,
                shape,
            )
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
        reopen_sized(name, 64 * 1024, 64 * 1024)
    }

    fn reopen_sized(name: &str, arena: usize, staging: usize) -> Local {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match Local::open(name, arena, staging, 4 * 1024 * 1024) {
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
    fn stale_region_debris_is_removed() {
        let name = instance("stale");
        let directory;
        {
            let first = Local::open(&name, 64 * 1024, 64 * 1024, 4 * 1024 * 1024).unwrap();
            directory = first.directory().to_path_buf();
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
        assert!(!directory.join("leftover.tmp").exists());
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

    fn inode(path: &Path) -> u64 {
        use std::os::unix::fs::MetadataExt;
        fs::metadata(path).unwrap().ino()
    }

    #[test]
    fn a_restart_reuses_the_same_files_and_offsets() {
        let name = instance("reuse");
        let (first, cleanup) = local(&name, 64 * 1024, 64 * 1024);
        let mut first = first;
        first.accept(1, std::process::id()).unwrap();
        let grant = first
            .grant_put(1, 1, 0, Region::Staging, shape(1, false))
            .unwrap();
        let (arena, staging) = (
            first.arena_path().to_path_buf(),
            first.staging_path().to_path_buf(),
        );
        let (generation, inodes) = (first.regions_generation(), (inode(&arena), inode(&staging)));
        assert_ne!(generation, 0);
        // The window is still granted when the manager goes away.
        drop(first);

        let mut second = reopen(&name);
        assert_eq!(second.arena_path(), arena);
        assert_eq!(second.staging_path(), staging);
        assert_eq!(
            (inode(&arena), inode(&staging)),
            inodes,
            "the files were reused"
        );
        assert_eq!(second.regions_generation(), generation);
        // The test process is the holder that was recorded, so it is held until it reconnects.
        assert!(second.held_for_previous_run());
        second.accept(1, std::process::id()).unwrap();
        assert!(!second.held_for_previous_run());
        let again = second
            .grant_put(1, 1, 0, Region::Staging, shape(1, false))
            .unwrap();
        assert_eq!(again.offset, grant.offset, "windows name the same offsets");
        drop(cleanup);
    }

    #[test]
    fn a_changed_size_recreates_the_files_under_a_new_generation() {
        let name = instance("resize");
        let (first, cleanup) = local(&name, 64 * 1024, 64 * 1024);
        let generation = first.regions_generation();
        let directory = first.directory().to_path_buf();
        drop(first);

        let second = reopen_sized(&name, 64 * 1024, 128 * 1024);
        let resized = second.regions_generation();
        assert_ne!(resized, generation);
        assert_eq!(second.staging_size(), 128 * 1024);
        assert_eq!(
            fs::metadata(second.staging_path()).unwrap().len(),
            128 * 1024
        );
        assert_eq!(
            fs::read_to_string(directory.join("generation"))
                .unwrap()
                .trim(),
            format!("{:016x}", second.regions_generation())
        );
        assert!(
            !second.held_for_previous_run(),
            "new files have no previous holders"
        );
        drop(second);
        // The same size again is a plain reuse.
        let third = reopen_sized(&name, 64 * 1024, 128 * 1024);
        assert_eq!(third.regions_generation(), resized);
        drop(cleanup);
    }

    #[test]
    fn an_unusable_file_is_recreated_under_a_new_generation() {
        let name = instance("unusable");
        let (first, cleanup) = local(&name, 64 * 1024, 64 * 1024);
        let generation = first.regions_generation();
        let staging = first.staging_path().to_path_buf();
        drop(first);
        // Truncated behind the manager's back.
        fs::OpenOptions::new()
            .write(true)
            .open(&staging)
            .unwrap()
            .set_len(4096)
            .unwrap();
        let second = reopen(&name);
        assert_ne!(second.regions_generation(), generation);
        assert_eq!(fs::metadata(&staging).unwrap().len(), 64 * 1024);
        drop(second);

        // A generation nobody can read is as unusable as a missing file.
        let directory = cleanup.0.clone();
        let second_generation = fs::read_to_string(directory.join("generation")).unwrap();
        fs::write(directory.join("generation"), b"not a number").unwrap();
        let third = reopen(&name);
        assert_ne!(
            format!("{:016x}\n", third.regions_generation()),
            second_generation
        );
        drop(third);
        fs::remove_file(directory.join("generation")).unwrap();
        let fourth = reopen(&name);
        assert!(directory.join("generation").exists());
        drop(fourth);
    }

    #[test]
    fn cleanup_never_removes_reused_or_live_or_another_instances_files() {
        let name = instance("clean-a");
        let other = instance("clean-b");
        let (first, cleanup) = local(&name, 64 * 1024, 64 * 1024);
        let (neighbour, other_cleanup) = local(&other, 64 * 1024, 64 * 1024);
        let directory = first.directory().to_path_buf();
        let (arena, staging) = (
            first.arena_path().to_path_buf(),
            first.staging_path().to_path_buf(),
        );
        let inodes = (inode(&arena), inode(&staging));

        // A second manager of the same instance is refused and touches nothing.
        fs::write(directory.join("debris"), b"x").unwrap();
        let refused = Local::open(&name, 64 * 1024, 64 * 1024, 4 * 1024 * 1024)
            .err()
            .unwrap();
        assert!(refused.to_string().contains("already in use"));
        assert!(
            directory.join("debris").exists(),
            "a live instance is not reclaimed"
        );
        assert_eq!((inode(&arena), inode(&staging)), inodes);

        // After it stops, the debris goes and the reused files stay.
        drop(first);
        let second = reopen(&name);
        assert!(!directory.join("debris").exists());
        assert_eq!((inode(&arena), inode(&staging)), inodes);
        // The neighbour, live throughout, kept every file.
        for file in ["arena", "staging", "generation", "lock"] {
            assert!(neighbour.directory().join(file).exists(), "{file}");
        }
        drop(second);
        drop(neighbour);
        drop(cleanup);
        drop(other_cleanup);
    }

    #[test]
    fn a_previous_runs_live_holder_keeps_the_regions_until_it_reconnects_or_exits() {
        let mut gone = spawn_sleeper();
        let mut back = spawn_sleeper();
        let name = instance("prior");
        let (mut first, cleanup) = local(&name, 64 * 1024, 64 * 1024);
        first.accept(1, gone.0.id()).unwrap();
        first.accept(2, back.0.id()).unwrap();
        first
            .grant_put(1, 1, 0, Region::Staging, shape(1, false))
            .unwrap();
        // The manager dies without closing anything.
        drop(first);

        let mut second = reopen(&name);
        assert!(second.held_for_previous_run());
        let flag = second.held_flag();
        assert!(flag.load(std::sync::atomic::Ordering::Acquire));
        second.accept(9, std::process::id()).unwrap();
        for region in [Region::Staging, Region::Arena] {
            assert_eq!(
                second.grant_put(9, 1, 0, region, shape(1, false)),
                Err(GrantError::Budget),
                "{region:?} is held for the previous run"
            );
            assert!(!second.can_allocate(region, 4096));
        }
        // One holder reconnects: it drained before it did, but the other has not been heard from.
        second.accept(3, back.0.id()).unwrap();
        assert!(second.held_for_previous_run());
        // The other exits.
        gone.0.kill().unwrap();
        gone.0.wait().unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while second.held_for_previous_run() {
            second.poll_exits(&mut Vec::new());
            assert!(Instant::now() < deadline, "the exit was never seen");
            thread::sleep(Duration::from_millis(2));
        }
        assert!(!flag.load(std::sync::atomic::Ordering::Acquire));
        second
            .grant_put(9, 1, 0, Region::Staging, shape(1, false))
            .unwrap();
        second
            .grant_put(9, 2, 0, Region::Arena, shape(1, false))
            .unwrap();
        drop(second);
        let _ = back.0.kill();
        let _ = back.0.wait();
        drop(cleanup);
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

        // SAFETY: these distinct fixture windows are exclusively owned by this test.
        unsafe {
            std::ptr::copy_nonoverlapping(
                b"AAAA".as_ptr(),
                left.region_mapping(Region::Arena).0.add(a.offset as usize),
                4,
            );
            std::ptr::copy_nonoverlapping(
                b"BBBB".as_ptr(),
                right.region_mapping(Region::Arena).0.add(b.offset as usize),
                4,
            );
        }
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
}
