# Kubernetes requirements

- Linux workers with protocol configuration: FC HBA/fabric zoning to the storage targets, SCSI utilities, and the expected target WWNs. Verify the host can discover the LUNs before deployment.
- Permit the privileged CSI node plugin, host root/device/sysfs access, and kubelet mount propagation used by the DaemonSet.
- Controller pods need NGX Storage management HTTPS connectivity. Workers need management access and protocol data-path connectivity.
- A backend pool enabled for FC and a valid runtime API key.
- Snapshot workflows need `snapshot.storage.k8s.io/v1` CRDs and a snapshot controller.
- Create Secret `fc-csi-ngxstorage-secret` with `storage-ips`, `pool-name`, `fc-target-names`, and `api-key`. Never commit credentials or print them in logs.
- The Kubernetes overlay has no SCC objects. Cluster policy must permit the node host access described in the security note.

The checked-in namespace is `ngxstorage-fc`. If changing it, inspect rendered namespaced and cluster-scoped references. See [manual deployment](../../../deploy/README.md).
