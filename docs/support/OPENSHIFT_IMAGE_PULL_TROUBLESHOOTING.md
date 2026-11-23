# OpenShift driver image pull troubleshooting

Inspect pod events and the exact rendered image reference. Deployment uses a registry-qualified immutable image digest; bare names and `latest` are not deployment inputs.

```bash
oc -n ngxstorage-fc get pods -o wide
oc -n ngxstorage-fc describe pod <pod>
oc -n ngxstorage-fc get pod <pod> -o jsonpath='{.spec.containers[*].image}{"\n"}'
```

Check registry reachability, credentials, repository access, digest existence, and the build record. Do not change cluster registry configuration to mask unresolved image or authentication errors.
