use super::PlannedRegion;

/// Remaining startup supply, after local tiers, runtime headroom and minimum caches.
pub struct Budget {
    pub host: std::result::Result<u64, String>,
    pub devices: Vec<(u64, bool)>,
}

impl Budget {
    pub(super) fn costs(
        mut bytes: impl Iterator<Item = u64>,
    ) -> std::result::Result<(u64, u64), String> {
        bytes.try_fold((0u64, 0u64), |(host, device), bytes| {
            Ok((
                host.checked_add(bytes).ok_or("pool host bytes overflow")?,
                device
                    .checked_add(bytes.div_ceil(512))
                    .ok_or("pool device bytes overflow")?,
            ))
        })
    }

    pub fn charges(&self, host: u64, device: u64) -> Vec<u64> {
        let integrated = self.devices.iter().filter(|(_, shared)| *shared).count() as u64;
        self.devices
            .iter()
            .map(|(_, shared)| {
                device.saturating_add(if *shared {
                    host.div_ceil(integrated)
                } else {
                    0
                })
            })
            .collect()
    }

    pub(super) fn approve(&self, plan: &[PlannedRegion]) -> std::result::Result<(), String> {
        let (host, device) = Self::costs(plan.iter().map(|region| region.bytes))?;
        let available_host = self
            .host
            .as_ref()
            .map_err(|error| format!("cannot budget pool host memory: {error}"))?;
        if host > *available_host {
            return Err(format!(
                "needs {host} host bytes and {device} device bytes per GPU; host budget has {} bytes left",
                available_host
            ));
        }
        for (rank, ((available, _), required)) in self
            .devices
            .iter()
            .zip(self.charges(host, device))
            .enumerate()
        {
            if required > *available {
                return Err(format!(
                    "needs {host} host bytes and {device} device bytes per GPU; GPU {rank} needs {required} bytes including shared host memory, but its budget has {available} bytes left"
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn plan(arena: u64, staging: u64) -> Vec<PlannedRegion> {
        vec![
            PlannedRegion {
                name: "arena",
                bytes: arena,
            },
            PlannedRegion {
                name: "staging",
                bytes: staging,
            },
        ]
    }

    #[test]
    fn approval_charges_whole_regions_and_each_gpu_before_registration() {
        let plan = plan(1 << 30, 1 << 29);
        let (host, device) = Budget::costs(plan.iter().map(|region| region.bytes)).unwrap();
        assert_eq!((host, device), (3 << 29, 3 << 20));
        let budget = Budget {
            host: Ok(host),
            devices: vec![(device, false); 2],
        };
        assert!(budget.approve(&plan).is_ok());
        assert_eq!(budget.charges(host, device), vec![device; 2]);
        assert!(
            Budget {
                host: Ok(host - 1),
                ..budget
            }
            .approve(&plan)
            .unwrap_err()
            .contains("host budget")
        );
        let budget = Budget {
            host: Ok(host),
            devices: vec![(device, false), (device - 1, false)],
        };
        assert!(budget.approve(&plan).unwrap_err().contains("GPU 1"));
        assert!(Budget::costs(self::plan(u64::MAX, 1).iter().map(|region| region.bytes)).is_err());
    }

    #[test]
    fn remote_staging_and_shared_memory_use_the_same_budget() {
        let plan = vec![PlannedRegion {
            name: "data-staging",
            bytes: 4097,
        }];
        assert_eq!(
            Budget::costs(plan.iter().map(|region| region.bytes)).unwrap(),
            (4097, 9)
        );
        let budget = Budget {
            host: Ok(4097),
            devices: vec![(2058, true), (2058, true), (9, false)],
        };
        assert_eq!(budget.charges(4097, 9), [2058, 2058, 9]);
        assert!(budget.approve(&plan).is_ok());
        assert!(
            Budget {
                host: Ok(4097),
                devices: vec![(2057, true), (2058, true)]
            }
            .approve(&plan)
            .is_err()
        );
        assert!(
            Budget {
                host: Err("no memory observation".into()),
                devices: Vec::new()
            }
            .approve(&plan)
            .unwrap_err()
            .contains("no memory observation")
        );
    }
}
