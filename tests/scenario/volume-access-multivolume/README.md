# High-count multivolume teardown

This OCP2 FC workload creates 16 distinct 1Gi PVCs, mounts all of them in one
worker-pinned Pod, and writes/reads a unique marker on every mount. It captures
each Pod UID/PV mount path, multipath UUID, and path WWN before deleting the
Pod once. PASS requires every VolumeAttachment to disappear and every captured
mount, map, and path WWN to be absent from that worker. The test never flushes
maps or logs out sessions itself.

Run: `BENCH=ocp2 bash tests/run-all-tests.sh scenario volume-access-multivolume --allow-live`.
Provide the runtime NGX Storage settings described in `tests/README.md` first.

MULTIVOLUME_COUNT may lower the count for isolated diagnostics (2–32); the
acceptance run uses the default 16. The test cleans up its uniquely named
StorageClass, PVCs, PVs, and Pod. Do not use --keep for teardown acceptance.
Record the running CSI image digest with the result: a live pass against a
deployed baseline does not validate unbuilt local source changes.
