# Split packed exports at shard boundaries

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at the TiDB repository root. This plan covers coordinated changes in the TiDB worktree `/DATA/disk3/juncen/developer/tidb_worktrees/exp-export-packed` and the paired TiKV worktree `/DATA/disk3/juncen/developer/tikv-worktree/exp-export-packed`.

## Purpose / Big Picture

Packed export currently creates one Dumpling data task per logical table. Although `cse-ctl` can scan several packed shards concurrently, one Dumpling writer decodes, serializes, and writes all rows of a large table. After this change, Dumpling asks `cse-ctl` for the intersections between a physical table key range and packed shard ranges, creates one data task for each intersection, and runs those tasks through the existing writer pool. A user can observe multiple chunk-indexed output files for a table spanning several packed shards and concurrent `/scan` requests without increasing the configured global packed snapshot concurrency.

## Progress

- [x] (2026-09-03) Reconstructed the current Dumpling and `cse-ctl` packed scan flows.
- [x] (2026-09-03) Chose a size-free `/sample` protocol that returns one range per intersecting packed shard.
- [x] (2026-09-03) Added manifest-only shard range sampling to `PackedBackupReader` and exposed it through `cse-ctl dumper`.
- [x] (2026-09-03) Added the Dumpling `/sample` client, exact-coverage response validation, and per-range task generation.
- [x] (2026-09-03) Made packed export output naming always include chunk index, including when file-size splitting is enabled.
- [x] (2026-09-03) Ran focused native reader, cse-ctl HTTP service, and Dumpling packed export tests successfully.
- [x] (2026-09-03) Ran required Ready validation and self-reviewed both diffs.
- [x] (2026-09-03) Built release `cse-ctl` and current Dumpling, then exported the complete local-k8s TPCC300 packed fixture successfully.

## Surprises & Discoveries

- Observation: A single current `/scan` already scans packed shards concurrently, but its result stream is forwarded in key order to one Dumpling writer.
  Evidence: `components/native_br/src/packed_reader.rs` uses `PackedScanPool::scan_tasks`, while `dumpling/export/packed.go` creates one `TaskTableData` per logical table.

- Observation: When only `FileSize` is configured, the generic output namer omits chunk index because normal Dumpling associates chunking with `Rows`.
  Evidence: `dumpling/export/writer.go:newOutputFileNamer` selects `%09[2]d` for `fileSize` without `rows`; packed configuration rejects `Rows`.

## Decision Log

- Decision: `/sample` accepts only `start_key_hex` and `end_key_hex` and returns one clipped half-open range per intersecting packed shard.
  Rationale: The first implementation is intended to measure the benefit of Dumpling-side parallelism before defining multi-shard grouping or byte weights.
  Date/Author: 2026-09-03, Codex and user.

- Decision: Reuse the reader's scan planning for sampling rather than independently walking manifest shards in the HTTP handler.
  Rationale: Sampling and scanning must use identical ordering, clipping, keyspace-prefix, and coverage-gap rules.
  Date/Author: 2026-09-03, Codex.

- Decision: Keep generic Dumpling behavior unchanged and force chunk-indexed names only for packed tasks.
  Rationale: Existing non-packed output naming is a compatibility contract. Packed export has no previous multi-task table output to preserve.
  Date/Author: 2026-09-03, Codex and user.

- Decision: Require paired Dumpling and `cse-ctl` versions for this experimental path; do not add a missing-endpoint fallback.
  Rationale: The requested proof of concept values a small, explicit protocol over compatibility machinery, and Dumpling launches the selected helper binary itself.
  Date/Author: 2026-09-03, Codex.

## Outcomes & Retrospective

The size-free, one-shard-per-range implementation is complete. Dumpling now samples each physical table key range, rejects any response that does not exactly and contiguously cover that range, and submits one independently writable task per sampled range. Packed data filenames always retain chunk index, including with file-size splitting. The helper derives sample boundaries from the same manifest-only planning used by scans, so `/sample` does not load snapshots or content objects.

Focused tests, TiDB `make lint`, TiKV `make format`, and TiKV `make clippy` passed. No Bazel preparation trigger was present. Mixed-version Dumpling/helper behavior was not tested; the protocol intentionally requires a matching helper version for this experiment.

The local-k8s TPCC300 fixture subsequently provided a real packed-backup run. With 8 threads, the full CSV export completed in 173.31 seconds, emitted 23,034,947,155 CSV bytes across 20 data chunks, and reached 126.75 MiB/s when output bytes are divided by wall time. Maximum RSS reported by `/usr/bin/time` was 8,029,396 KiB. The export contained 149,819,627 data rows across 9 tables. Multi-shard tables produced contiguous chunk-indexed files: `customer` used 4 chunks, `order_line` used 3, and `stock` used 7. The `warehouse` data and schema SHA256 hashes matched the established fixture baseline exactly. No single-shard baseline was run in the same environment, so this result validates behavior and supplies an absolute throughput measurement but does not establish a speedup ratio.

## Context and Orientation

In TiDB, `dumpling/export/packed_protocol.go` owns the HTTP/2 Unix-socket client for `cse-ctl dumper`. `dumpling/export/packed.go` discovers TiDB metadata from raw packed KV ranges, builds physical record ranges for ordinary and partitioned tables, and sends `TaskTableData` objects to writers. A physical table range is a half-open raw TiDB record-key interval. A chunk index is the deterministic per-table number embedded in output filenames.

In TiKV, `cmd/cse-ctl/src/dumper.rs` owns the HTTP endpoints. `components/native_br/src/packed_reader.rs` owns packed manifest validation, API V2 keyspace-prefix conversion, shard ordering, range coverage checks, snapshot creation, and raw KV iteration. A packed shard is a manifest snapshot with one half-open outer-key range. Sampling must not create snapshots or read SST objects.

## Plan of Work

First, refactor `PackedBackupReader` range planning so a public sampling method can return inner-key ranges derived from the same `PackedScanTask` sequence used by scans. Add tests for clipping, ordering, complete coverage, and malformed manifest gaps. Add `POST /sample` to `cse-ctl dumper`, encode the sampled ranges as hexadecimal JSON, and test the HTTP route.

Second, add request and response types plus a `sample` method to the Dumpling client. Validate that the helper response contains non-empty, ordered, contiguous ranges which exactly cover the requested interval. Extend the packed test HTTP server to serve both `/sample` and `/scan`.

Third, sample every physical record range before sending a table's data tasks. Flatten results in physical-range order, assign continuous chunk indices across the logical table, and give each `packedTableData` exactly one sampled range. Empty tables may still create empty shard-intersection tasks; writers already suppress empty output files.

Finally, pass a packed-only chunk-index naming signal into `Writer` without changing non-packed naming. Add coverage for the `FileSize` case so two packed chunks cannot select the same output filename.

## Concrete Steps

From the TiKV worktree, format and run targeted tests:

    cargo test -p native_br packed_reader::tests
    cargo test -p cse-ctl dumper::tests
    make format
    make clippy

From the TiDB worktree, inspect the Bazel prepare gate after edits, run `make bazel_prepare` if required, then run targeted tests and the Ready lint gate:

    go test ./dumpling/export -run 'TestPacked' -tags=intest,deadlock
    make lint

Do not run `make bazel_lint_changed`.

## Validation and Acceptance

The native reader test must show that a request crossing several shard ranges returns the exact clipped inner-key intersections and reports a coverage gap using the existing planning error. The `cse-ctl` test must show that `POST /sample` returns JSON and performs no scan streaming.

The Dumpling protocol test must reject malformed sample responses which have a gap, overlap, empty range, or changed outer bounds. The packed export test must show one `/scan` request per sampled shard range, complete exported rows with no duplicates or omissions, deterministic chunk files, and distinct names when `FileSize` is enabled.

All targeted commands and required Ready checks must exit successfully. Generated Bazel metadata, if any, must be reviewed and kept minimal.

## Idempotence and Recovery

All tests and formatting commands are safe to rerun. The implementation changes only source and test files. If a test fails after a partial edit, keep the living `Progress` section accurate and rerun the narrowest failing test. Do not discard unrelated user changes with Git commands.

## Artifacts and Notes

Both worktrees were clean before implementation. No persistent external service or data is required because existing tests use in-memory storage and Unix sockets under temporary directories.

Focused validation completed successfully:

    cargo test -p native_br packed_reader::tests --lib
    cargo test -p cse-ctl dumper::tests
    ./tools/check/failpoint-go-test.sh dumpling/export -run 'Test(PackedProtocolRows|DumpPackedFromTiDBStorage)$' -count=1

The Bazel prepare gate found no changed Go import blocks, added Go files, new top-level test functions, Bazel files, or module files, so `make bazel_prepare` is not required.

Real fixture validation completed successfully against metadata object
`juncen-native-br/backup/tpcc300/7628844331459966093/_meta/20261504/070237.meta`:

    make build_dumpling
    cargo build --release -p cse-ctl
    ./bin/dumpling --cse.packed-backup <metadata-path> --cse.ctl-path <release-cse-ctl> --threads 8 --filetype csv --output <temporary-output-dir> --status-addr ''

The output is retained at
`/DATA/disk3/juncen/developer/tidb_worktrees/exp-export-packed/.tmp-packed-export-tpcc300-lXzM0q`, with the Dumpling log in the sibling `.log` file. Credentials were passed only through the process environment and were not written to this plan or the repository.

## Interfaces and Dependencies

`PackedBackupReader` will expose a synchronous method equivalent to:

    pub fn sample_range(&self, start_inner: &[u8], end_inner: &[u8]) -> Result<Vec<Range<Vec<u8>>>>

`cse-ctl` will accept:

    POST /sample
    {"start_key_hex":"...","end_key_hex":"..."}

and return:

    {"ranges":[{"start_key_hex":"...","end_key_hex":"..."}]}

Dumpling's packed scanner abstraction will support both sampling and scanning. Each returned range becomes one `TaskTableData`, and the packed-only naming signal ensures chunk index is retained independently of `Rows`.
