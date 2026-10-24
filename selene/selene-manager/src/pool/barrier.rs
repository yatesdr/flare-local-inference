//! The expected-source barrier: the instance ids the manager served, persisted
//! in its store directory so a restarted manager knows whom to wait for.
//!
//! The file is replaced atomically (write a temporary file, sync, rename, sync
//! the directory), so a crash mid-write leaves the previous list.

use super::index::InstanceId;
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{self, Write},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const FILE: &str = "instances";
const HEADER: &str = "selene-instances-v2";

/// Instance ids with the unix time each was last seen.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Remembered {
    seen: BTreeMap<InstanceId, u64>,
}

impl Remembered {
    pub fn load(directory: &Path, retention: Duration, now: SystemTime) -> io::Result<Self> {
        let text = match fs::read_to_string(directory.join(FILE)) {
            Ok(text) => text,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(error) => return Err(error),
        };
        let mut seen = BTreeMap::new();
        let mut lines = text.lines();
        if lines.next() != Some(HEADER) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{} is not a selene instance list",
                    directory.join(FILE).display()
                ),
            ));
        }
        let oldest = unix(now).saturating_sub(retention.as_secs());
        for line in lines {
            let Some((id, time)) = line.split_once(' ') else {
                continue;
            };
            let (Some(id), Ok(time)) = (parse_id(id), time.trim().parse::<u64>()) else {
                continue;
            };
            if time >= oldest {
                seen.insert(id, time);
            }
        }
        Ok(Self { seen })
    }

    pub fn ids(&self) -> impl Iterator<Item = InstanceId> + '_ {
        self.seen.keys().copied()
    }

    pub fn len(&self) -> usize {
        self.seen.len()
    }

    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }

    pub fn contains(&self, id: &InstanceId) -> bool {
        self.seen.contains_key(id)
    }

    /// Records that `id` is alive now. Returns whether the list changed enough to persist.
    pub fn touch(&mut self, id: InstanceId, now: SystemTime) -> bool {
        let now = unix(now);
        let previous = self.seen.insert(id, now);
        previous != Some(now)
    }

    pub fn forget_older_than(&mut self, retention: Duration, now: SystemTime) {
        let oldest = unix(now).saturating_sub(retention.as_secs());
        self.seen.retain(|_, time| *time >= oldest);
    }

    pub fn save(&self, directory: &Path) -> io::Result<()> {
        let path: PathBuf = directory.join(FILE);
        let temporary = directory.join(format!(".{FILE}.tmp"));
        let mut text = String::from(HEADER);
        text.push('\n');
        for (id, time) in &self.seen {
            text.push_str(&format!("{} {time}\n", hex(id)));
        }
        let mut file = File::create(&temporary)?;
        file.write_all(text.as_bytes())?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary, &path)?;
        File::open(directory)?.sync_all()
    }
}

fn unix(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn parse_id(text: &str) -> Option<InstanceId> {
    if text.len() != 32 {
        return None;
    }
    let mut id = [0u8; 16];
    for (slot, pair) in id.iter_mut().zip(text.as_bytes().chunks(2)) {
        *slot = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }
    Some(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn directory(name: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("selene-barrier-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn round_trip_and_retention() {
        let dir = directory("round");
        let now = UNIX_EPOCH + Duration::from_secs(1_000_000);
        let mut list = Remembered::default();
        list.touch([1; 16], now - Duration::from_secs(10));
        list.touch([2; 16], now - Duration::from_secs(8 * 86_400));
        list.save(&dir).unwrap();
        let week = Duration::from_secs(7 * 86_400);
        let loaded = Remembered::load(&dir, week, now).unwrap();
        assert!(loaded.contains(&[1; 16]));
        assert!(
            !loaded.contains(&[2; 16]),
            "unseen beyond retention is forgotten"
        );
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn missing_file_is_empty_and_garbage_is_an_error() {
        let dir = directory("missing");
        let now = SystemTime::now();
        assert!(
            Remembered::load(&dir, Duration::from_secs(60), now)
                .unwrap()
                .is_empty()
        );
        fs::write(dir.join(FILE), "nope\n").unwrap();
        assert!(Remembered::load(&dir, Duration::from_secs(60), now).is_err());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn a_crash_mid_write_leaves_the_previous_list() {
        let dir = directory("crash");
        let now = SystemTime::now();
        let mut list = Remembered::default();
        list.touch([7; 16], now);
        list.save(&dir).unwrap();
        // A torn temporary file from a crash is ignored and replaced by the next save.
        fs::write(dir.join(format!(".{FILE}.tmp")), "selene-instances-v2\nxx").unwrap();
        let loaded = Remembered::load(&dir, Duration::from_secs(60), now).unwrap();
        assert!(loaded.contains(&[7; 16]));
        let mut next = loaded.clone();
        next.touch([8; 16], now);
        next.save(&dir).unwrap();
        let loaded = Remembered::load(&dir, Duration::from_secs(60), now).unwrap();
        assert!(loaded.contains(&[7; 16]) && loaded.contains(&[8; 16]));
        let _ = fs::remove_dir_all(dir);
    }
}
