# benchmark-kbench (longhornio/benchmark-kbench) — FC CSI driver

`longhornio/benchmark-kbench` ile NGX Storage FC CSI driver'ın performans doğrulaması:
randread/randwrite IOPS, seq read/write bandwidth, read/write latency.

## Çalıştırma

```bash
# FC yalnızca: kendi namespace/SC/PVC/job isimleri
oc apply -f benchmark-kbench-namespace.yaml -f benchmark-kbench-serviceaccount.yaml -f benchmark-kbench-sc.yaml -f benchmark-kbench-pvc.yaml -f benchmark-kbench-job.yaml

# job bitene kadar bekle (MODE=full ~12-35 dk)
oc wait --for=condition=complete job/benchmark-kbench-fc -n benchmark-kbench-fc --timeout=1200s

# sonucu al
oc logs job/benchmark-kbench-fc -n benchmark-kbench-fc --tail=40

# Aynı akışı script ile de çalıştırabilirsin
bash run-benchmark-kbench.sh
```

The job has a dedicated service account. Do not grant `anyuid` to the default
service account; the driver image must provide the requested mount permissions
before this benchmark is run under restricted SCC.

## Beklenen çıktı

```
FIO Benchmark Summary
=========================
IOPS (Read/Write)
        Random:          <FC ölçümü>
Bandwidth in KiB/sec (Read/Write)
    Sequential:        <FC ölçümü>
Latency in ns (Read/Write)
        Random:          <FC ölçümü>
```

FC ölçümü henüz yapılmadı; çalıştırıldığında `tests/scenario/benchmark-kbench/logs/` kanıt dizinine
yazılacak.

## Karşılaştırma referansları

| Driver | IOPS R/W | Seq BW KiB/s R/W | Latency ns R/W |
|--------|----------|------------------|----------------|
| NFS 40G | 104,113 / 535 | 2,207,924 / 44,470 | 77,378 / 10,424,701 |
| iSCSI 40G | 117,464 / 21,523 | 33,800 / 1,492,617 | 75,234 / 244,264 |
| FC 40G | — | — | — |

## Notlar
- Sonuçlar `tests/scenario/benchmark-kbench/logs/` altında saklanır.
- Seq read değeri LUN'un okuma cache durumuna bağlıdır (write flash-cache'li, read HDD);
  derin queue (iodepth=16) ile 2.8 GB/s okuma görülür, benchmark-kbench iodepth=1 kullanır.
