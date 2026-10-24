# Qualification

What each test suite in this crate proves, how to run it, and which cases of the pool
protocol's test list (ids such as H5, C7, F3) it covers. Cases that need an instance (the
`selene` client crate) or a GPU (Flare) are not here: they are exercised in those repositories.

## Test ladder

Each rung uses the real component below it and adds one class of risk. All of them run on a
Linux CPU host against a real manager; the pool tests drive it with a minimal wire-level
client (`tests/support/pool.rs`) and never model the manager's state machine.

| Suite | What it proves |
|---|---|
| Unit (`src/**`) | Modules in isolation: the index and chain walks, window planning, the instance-list file, configuration, the record format, the arena, the disk worker, the store's admission and eviction rules, the data-channel modules, and the store-claim completion order. |
| `tests/pool_l0.rs` | Hello, sessions and supersede, notices, generations, resync (every failure), lookup, readiness and the restart barrier, limits, shutdown drain. |
| `tests/pool_claims.rs` | Claims between instances: retained and streamed copies byte for byte through the shared staging region, every refusal, timeouts, races, fencing and quarantine, Goodbye, stream blips, process replacement, and a real peer process that is killed or hung. |
| `tests/pool_store.rs` | Put into the manager's store and claims on its records: persisted checksums, recovered NVMe records, damage at rest, a requester whose stream drops, put cancel and fence. |
| `tests/pool_net.rs` | The network listener: sessions without shared memory, level-5 candidates, refusal of the data channel, TLS declared and RDMA withheld, the startup warnings and the refusal of RDMA with TLS. |
| `tests/l0.rs` | The store engine through the pool protocol: admission and refusal classification, eviction and compaction, chain durability, crash and recovery, streaming and oversize records, budget and cancel, directory modes, failed-start cleanup, signal handling, kill/restart, and a throughput smoke test. |
| `tests/proto.rs` | The proto file is the one both ends were built from (digest). |

## Running

```sh
cargo test --release --locked
cargo test --release --locked --features rdma
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
```

Set `SELENE_NVME_SCRATCH` to a disposable directory on a local NVMe file system for the tests that
need real direct I/O. The tests that start managers need room in `/dev/shm`; in a container pass
`--shm-size=4g`. The `rdma` feature needs libibverbs and a CMake toolchain at build time (see
`Dockerfile.rdma`). RDMA is experimental and is not a correctness gate.

## Test list coverage

Manager-side cases of the pool protocol's test list, with the test that covers each. "client"
means the case belongs to the instance-side crate; "Flare" means it needs the GPU runtime.

**Hello, versions, encryption.** H1 `h1_a_v1_hello_is_a_version_mismatch`; H2, H3, H9 client;
H4 `h4_a_tls_manager_declares_tls_and_offers_no_rdma`, `rdma_with_tls_is_refused_at_startup_with_the_reason`;
H5 `h5_a_stale_incarnation_is_refused_and_a_new_hello_succeeds`;
H6 `h6_a_unix_peer_of_the_same_user_gets_shared_memory`, `h6_a_tcp_session_has_no_shared_memory_and_serves_no_exports`;
H7 `h7_malformed_scopes_are_refused`; H8 `h8_scopes_never_see_each_other`;
H10 `h10_a_reconnect_supersedes_the_old_stream`.

**Index notices and resync.** I1 to I4 `i1_to_i4_notices_apply_in_order_and_gaps_demand_a_resync`;
I5 to I7 `i5_to_i9_generations_spills_compactions`; I8 `i8_a_compacted_node_is_no_longer_an_endpoint_but_the_chain_holds`;
I9 `i9_a_leaf_leaves_its_page_only_ancestors_until_they_are_named`;
I10, I14, A15 `i10_to_i15_snapshots_install_atomically`; I11 to I13 `i11_to_i13_bad_resyncs`;
I15 `i15_a_snapshot_above_the_entry_bound_overflows`; I16, I17 client;
I18, I19 `i18_and_i19_stale_incarnations_and_session_end`; A13 `a13_a_stalled_resync_times_out_without_holding_anyone`;
A23 `i5_to_i9_generations_spills_compactions`.

**Lookup.** L1 to L3 `l1_to_l3_ordering_trimming_and_the_requesters_own_entries`;
L4 `l4_instance_candidates_need_shared_memory_on_both_sides`, `l4_and_a22_remote_sessions_see_only_the_managers_records_as_level_five`;
L5 to L8 `l5_to_l8_holes_limits_and_validation`, `x4_batch_chunk_and_chain_limits_are_enforced`;
L9 `l9_not_ready_answers_with_a_retry_hint`; L10 to L15 client; A22 as L4.

**Claim state machine.** C1 `c1_a_gpu_chain_is_copied_through_retained_windows`, `c1b_small_windows_cut_records_and_carry_the_row_last`;
C2 `c2_a_host_tier_claim_larger_than_staging_streams_with_credits`; C3 `u1_and_c3_a_put_is_stored_indexed_and_served_to_a_claim`;
C4 `c4_a_recovered_record_is_served_from_nvme_with_its_persisted_checksums`;
C5 `c5_source_refusals_fail_the_claim_and_free_the_slot`; C6 `c6_no_pin_answer_times_out_and_a_late_pin_is_released`;
C7, C8 `c7_and_c8_a_release_that_crosses_the_pin_ack_never_grants` (C9 is the source's half: Flare);
C10 `f2_...`, `f3_...` (a release after the grant; the requester-side rule is client);
C12 `c13_duplicates_change_nothing` (the first terminal wins, one terminal reply); C13 as C12;
C14 to C19 `c14_to_c19_invalid_unknown_and_replayed_claims`, `a10_...`, `a11_...`;
C15 `c15_copy_ok_with_an_unreleased_window_is_a_copy_error`; C16 `c16_deadline_and_no_progress_expire_a_claim`;
C17 `c17_a_failed_fill_fails_the_claim`; C20 `c20_a_gpu_export_that_cannot_be_reserved_fails_before_any_request`;
C21 `r13_shutdown_drains_claims_then_closes_sessions`; C22, C23 client. C11 has no manager-side
behaviour to test (an unknown claim creates no state).

**Export on the source.** E1 to E11, E13 to E17 are the source's behaviour (Flare/client);
E12 manager half `x1_claims_in_flight_and_export_slots_are_limited`.

**Fencing and quarantine.** F1 `f1_the_window_order_is_enforced`; F2 `f2_a_window_the_source_may_still_write_waits_for_its_end`;
F3 `f3_a_window_the_requester_may_still_read_waits_for_its_fence`; F4 `f4_a_quarantined_party_keeps_its_windows_out_of_service`;
F5 `r7_and_r8_a_killed_or_hung_requester_leaves_nothing_behind`, `a5_...`; F12 `s4_and_s7_a_new_process_attests_nothing_for_its_predecessor`;
F6 is not applicable yet (remote windows exist only with the data channel); F7 to F11, F13 client.

**Restart and death.** R2, R3, R6 `r2_r3_and_s2_ready_when_remembered_sources_return_or_the_wait_ends`;
R3b unit `a_crash_mid_write_leaves_the_previous_list`; R5 `i18_and_i19_...` (messages from a dead incarnation are dropped);
R7, R8 `r7_and_r8_...`; R10 `r10_a_killed_source_fails_its_claims_and_frees_its_windows`;
R11 `r11_a_source_blip_quarantines_until_the_export_end_arrives_on_a_new_session`;
R13 `r13_...`; R1, R4, R9, R12, R14 client.

**Two-source plan.** P1 to P13 client/Flare.

**Limits.** X1 `x1_...`; X2 `c5_...`, `x1_...` (refusals are immediate); X4 `x4_...`, `l5_to_l8_...`;
X5 `x5_a_deadline_is_clamped_to_the_managers_limit`; X3 client.

**Put.** U1 `u1_...`; U2 `u2_u8_and_a9_present_queue_and_invalid_metadata`; U4 `u4_a_cancelled_put_keeps_its_unfilled_windows_until_the_writers_fence`;
U8 as U2; U3, U6, U7 client; U5 (manager restart mid-put) is covered by the shutdown and restart tests
only in that the writer keeps its copy until `Stored`; there is no separate test.

**Flare hooks.** K1 to K8 Flare.

**Added after review.** A1, A2 `a1_and_a2_goodbye_clears_only_the_senders_obligations`;
A5 `a5_fence_records_outlive_the_reply_and_replays_stay_refused`;
A6 `a6_a_premature_or_partial_copy_ok_is_refused`; A7 `c1_...` (exact coverage accepted);
A8 `a8_damage_at_rest_never_reaches_a_restore` (a record read from NVMe into DRAM is verified by the store and
the claim fails `COPY_ERROR`; a window streamed straight from NVMe would be served with the persisted checksums);
A9 `u2_u8_...`; A10 `a10_...`; A11 `a11_...`; A21 `a21_a_pin_larger_than_the_reservation_fails_no_windows`;
A3, A4, A12, A14, A16 to A20 client/Flare.

**Stable instance id.** S2 `r2_r3_and_s2_...`; S3 `h10_...`, `s4_and_s7_...`; S4, S7 `s4_and_s7_...`;
S5, S6, S9 `s5_and_s6_the_predecessors_entries_are_hidden_until_the_new_snapshot`;
S8 `s8_a_changed_instance_id_does_not_satisfy_the_barrier_and_ages_out`; S1 client.

## Limitations

- The data channel is not served yet: claims and puts in `DATA_CHANNEL` mode are refused, and no
  test moves bytes between hosts. The stream and RDMA data-channel modules keep their unit tests.
- Process death is exercised with real `SIGKILL` and `SIGSTOP` of a peer process; a peer on another
  host (whose exit cannot be observed) has no windows to quarantine until the data channel exists.
- The tests cover the CPU data path and shared-memory windows. They do not include GPU transfer,
  scheduling or model execution time.
