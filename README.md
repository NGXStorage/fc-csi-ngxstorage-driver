# NGX Storage Fibre Channel CSI Driver

The NGX Storage Fibre Channel CSI driver provisions LUNs from the array and presents them to Kubernetes workloads over Fibre Channel. A pod can mount a filesystem on a LUN or request the raw block device.

## Features

- Dynamic provisioning of LUNs from a selected storage pool.
- Filesystem and raw block volumes.
- Snapshots and volume expansion through the CSI API.
- Kubernetes and OpenShift deployments with manifests or Helm charts.
- Standalone build from the committed Go dependencies.

## Prerequisites

Zone each storage worker's HBA initiators to the intended target ports and configure multipath on the workers. Both the controller and node plugins need access to the array management API, and you must provide the target names and pool used for provisioning.

| Platform | Requirements |
| --- | --- |
| Kubernetes | [Worker and storage requirements](docs/deployment/kubernetes/REQUIREMENTS.md) |
| OpenShift | [Worker, storage and SCC requirements](docs/deployment/openshift/REQUIREMENTS.md) |

## Install

| Platform | Installation |
| --- | --- |
| Kubernetes | [Manifests](docs/deployment/kubernetes/manifest/DEPLOYMENT.md) · [Helm](docs/deployment/kubernetes/helm/DEPLOYMENT.md) |
| OpenShift | [Manifests](docs/deployment/openshift/manifest/DEPLOYMENT.md) · [Helm](docs/deployment/openshift/helm/DEPLOYMENT.md) |

The manifest bundle registers `fc.csi.ngxstorage.com` and creates the StorageClass `fc-csi-ngxstorage`.

## First volume

Once the controller and node pods are ready, [provision a volume and verify that a pod can write to and read from it](docs/first-volume.md).

## Build and test

Run `make build` to compile the driver from the committed Go dependencies. For container images, test commands and configuration, see the [build guide](build/README.md), the [test guide](tests/README.md) and the [documentation index](docs/README.md).

## License

Licensed under [Apache 2.0](LICENSE).
