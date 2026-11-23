#!/usr/bin/env bash
set -euo pipefail

oc apply -f benchmark-kbench-namespace.yaml
oc apply -f benchmark-kbench-serviceaccount.yaml
oc -n benchmark-kbench-fc delete pod benchmark-kbench-fc --ignore-not-found --wait=false
oc apply -f benchmark-kbench-sc.yaml -f benchmark-kbench-pvc.yaml -f benchmark-kbench-pod.yaml
oc wait --for=jsonpath='{.status.phase}'=Bound pvc/benchmark-kbench-fc-pvc -n benchmark-kbench-fc --timeout=10m
oc wait --for=jsonpath='{.status.phase}'=Succeeded pod/benchmark-kbench-fc -n benchmark-kbench-fc --timeout="${KBENCH_TIMEOUT:-1200}s"
oc logs pod/benchmark-kbench-fc -n benchmark-kbench-fc
oc describe pod/benchmark-kbench-fc -n benchmark-kbench-fc | tail -40
