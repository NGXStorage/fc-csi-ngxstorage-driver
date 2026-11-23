# FC multipath attach and host teardown

Run one scenario with `BENCH=ocp2 bash tests/run-all-tests.sh scenario volume-attach-multipath --allow-live`.
Provide the runtime NGX Storage settings described in `tests/README.md` first.
It provisions a 1 GiB test volume, verifies marker I/O and at least two FC
paths to one WWN, then deletes the test Pod. A PASS also requires the
VolumeAttachment to disappear and the Pod mount plus the exact recorded
`mpath-*` device-mapper UUID and path WWN to be absent on that worker. The pre/post host
identity evidence is saved under the run's `logs/.../host/` directory. The
test never flushes a map directly; a failure leaves host residue for review.
This scenario uses a minimal StorageClass: `blocksize="128k"` and
`qos_priority=16` were separately rejected by the OCP2 array with code 5008
on 2026-09-29. Those API/parameter checks need their own investigation and
are not treated as host-teardown failures.

`--keep` preserves the Pod, so it intentionally skips teardown validation.
That run is attach-only evidence, not a host-teardown acceptance result.
The running CSI node image must correspond to the source revision being
accepted; an older deployment cannot validate local teardown changes.
