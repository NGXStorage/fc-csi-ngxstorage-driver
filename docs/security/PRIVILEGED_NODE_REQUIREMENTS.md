# Privileged node requirements for NGX Storage FC CSI

The node plugin runs privileged because it performs host-level device and mount operations. The DaemonSet accesses host root, kubelet paths, and device resources; the controller does not need those host mounts.

Protocol-specific work includes FC HBA/fabric zoning to the storage targets, SCSI utilities, and the expected target WWNs. Verify the host can discover the LUNs before deployment.

| Component | Host access | Purpose |
| --- | --- | --- |
| Controller | No host mount/device access | CSI control plane and NGX Storage management API operations |
| Node plugin | Privileged host access | Device discovery and volume mount lifecycle |

OpenShift uses `fc-csi-ngxstorage-node-scc` through its node ServiceAccount. Verify the effective pod security context after deployment; SCC YAML alone is not certification approval.
