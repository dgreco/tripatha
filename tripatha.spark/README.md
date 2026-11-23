# The Spark build

`tripatha.spark` loads a Tripatha store on Accumulo as a Spark job. It is the loader's bulk build (`run.sh build`, [SCALE-STUDY.md](../SCALE-STUDY.md) O3) spread over a cluster's executors instead of one machine's threads. It reads one of two inputs:

- **`generate`:** the loader's generated dataset (the same entities, triples and options as `run.sh load` and `build`), into a store that `run.sh prepare` prepared;
- **`files`:** N-Triples and N-Quads files (`.nt`, `.nq`, `.ntriples`, `.nquads`, optionally `.gz` or `.bz2`; RDF 1.2 included), into a new store, or with `--append` into one that holds statements.

It needs Spark 4.2 on Java 25 (Spark 4.2 runs on Hadoop 3.5, whose client works on Java 25) and Accumulo 2.1.

## How it works

1. **The driver plans.** It checks the store (for `generate`, that `prepare` made it for the same dataset and indexes), samples the input, and fixes each table's files.
   - **On a new store,** it adds split points as `prepare` does: each table's estimated size on disk over `--tablet-size`, at least 4 × tablet servers − 1.
   - **Each table's files** are its tablets, cut further at rows of the sample so that a file holds about `--file-size` of cells, with at most the table's `table.file.max` files per tablet (15 by default), since Accumulo compacts a tablet with more.
2. **The executors write the cells `load` would write.** Each task writes its statements through Tripatha's own DAO, over a store that only captures what the DAO writes, so every cell is the one an online load writes. That includes every index and counter, the datatype markers and the graph markers.
3. **Spark's shuffle sorts the cells.** Each task first adds up its counter deltas across its write calls. The shuffle then only sorts each partition in Accumulo's key order, and each file's writer adds up what is left and keeps a put's newest version. A sort alone costs less than Spark's own combining, which hashes every cell.
4. **Each partition becomes one RFile** under `--work-dir`, written with its table's configuration. Totals of 1 are left out in the six counter families but `p`, as the loader's build leaves them out (the planner reads a missing counter as 1), and only when the totals are the store's: a new store or a generated dataset. Even then, a 1 stays when it isn't its counter's only cell (another label's cell is added to it by a reader with both labels), and when the reference data `prepare` wrote counts that counter too. A generated dataset can also be built in parts, each with a work directory of its own (`--part 0 --parts 5`, then 1 to 4): a part leaves out only subject counters of 1, since other parts may add to its other counters.
5. **The driver imports the files,** table by table, with load plans that name each file's rows.

Only the finished RFiles go to S3. The shuffle's spills stay on the executors' local disks, and the executors need no access to Accumulo or its credentials, only to the work directory's file system.

**The work directory records the job:** `job.properties` holds its input's fingerprint, its timestamp and its state, and `manifest.properties` the files' rows.
- **The write phase** can be run again: it starts over and writes the same cells. There is one timestamp per job, and files are named after their table and range, never after a task.
- **Once an import has started,** the files are never written again, since Accumulo adds imported counters to what a table holds. A file that is gone was imported, so `--phase import` after a failure imports what is left.

**Blank nodes:** a label is scoped to its file, but one file is read in parts by different tasks. Each label is kept and prefixed with the file's id (a hash of its path, length and modification time), so the parts of a file agree on a label and two files never share one.

**Repeated statements:** without `--distinct`, a statement that appears twice is stored once but counted twice, as with `load`. `--distinct` removes repeats first, as the store tells statements apart (by their spo cell), at the cost of one more shuffle.

**Bad lines** (a line that doesn't parse, or a statement the store refuses) are logged and left out, up to `--max-bad-lines` (0 by default), and then the job stops.

## Requirements on the cluster

- **The work directory must be on one of Accumulo's volumes** (`instance.volumes`): Accumulo imports only from a file system it stores on, and moves the files into the table.
  - **With Accumulo on a shared POSIX volume** (CephFS or NFS, mounted in every pod, `instance.volumes=file:///var/accumulo/data`): mount the same volume in the Spark pods at the same path, and use a directory on it, for instance `file:///var/accumulo/data/tripatha-bulk/run-1`. The import is then a rename. See [Run on Kubernetes without a Spark image](#run-on-kubernetes-without-a-spark-image).
  - **With Accumulo on S3:** use a prefix of the same bucket, for instance `s3a://accumulo/tripatha-bulk/run-1`.
- **With S3, the executors need Hadoop's S3 connector** (`hadoop-aws` 3.5.0 and the AWS SDK bundle it names) and its settings. With Ceph or another S3-compatible store, path-style access is usually needed:

  ```
  spark.hadoop.fs.s3a.endpoint            https://rgw.example.internal
  spark.hadoop.fs.s3a.path.style.access   true
  spark.hadoop.fs.s3a.endpoint.region     us-east-1
  ```

  Credentials go through the usual S3A providers: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` from a Kubernetes secret, or `fs.s3a.access.key` and `fs.s3a.secret.key`. **The executors need them as well as the driver:** they write the RFiles. If `smoke` fails against Ceph with checksum or HTTP 400 errors, see Hadoop's notes on [S3A with third-party stores](https://hadoop.apache.org/docs/r3.5.0/hadoop-aws/tools/hadoop-aws/third_party_stores.html).
- **The executors need local disk for the shuffle:** about 150 bytes per triple across the executors, compressed, plus the sort's spills (at 500M triples, about 100 GB). Kubernetes' default `emptyDir` and ephemeral-storage limits can evict pods in the middle of a job. Give the executors volumes named `spark-local-dir-*`, for instance a persistent volume claim per executor (Spark's [local storage](https://spark.apache.org/docs/4.2.1/running-on-kubernetes.html#local-storage)).
- **The driver needs Accumulo:** ZooKeeper and the tablet servers, `--accumulo-properties FILE` (the loader's client properties; the password can come from `TRIPATHA_ACCUMULO_PASSWORD`), and an account that can create tables, add splits and bulk-import. It holds the plan's sample (up to 200,000 rows per table): give it 4 GB (`spark.driver.memory`).
- **No speculative execution** (`spark.speculation`, off by default): two attempts of a task would write one file at once. The job refuses to start with it on.

## Build

```bash
mvn -pl tripatha.spark -am package -DskipTests -DskipITs
# the job's jar: tripatha.spark/target/tripatha.spark-1.0.0-SNAPSHOT-job.jar
```

The jar holds Tripatha, Accumulo's client and RDF4J. Spark, Scala, Hadoop and the logging are the cluster's. Thrift, Guava, Gson, Protobuf and Caffeine are relocated under `org.tripatha.shaded`, since Spark's own jars carry other versions.

## Run on Kubernetes

**First, the smoke test.** Each executor writes a small RFile under the work directory, and the driver imports the files into a scratch table, reads them back and drops the table. It prints each executor's Java and Hadoop versions and file system, and it fails where a build would: executors that can't run the code or reach S3, or a directory Accumulo can't import from.

```bash
spark-submit \
  --master k8s://https://KUBERNETES_API:6443 --deploy-mode cluster \
  --name tripatha-smoke \
  --class org.tripatha.spark.SparkBuild \
  --conf spark.kubernetes.container.image=YOUR_SPARK_4.2_JAVA_25_IMAGE \
  --conf spark.driver.memory=4g \
  --conf spark.kubernetes.driver.secrets.tripatha-accumulo=/etc/tripatha \
  --conf spark.kubernetes.driver.secretKeyRef.TRIPATHA_ACCUMULO_PASSWORD=tripatha-accumulo:password \
  --conf spark.kubernetes.driver.secretKeyRef.AWS_ACCESS_KEY_ID=s3-credentials:access-key \
  --conf spark.kubernetes.driver.secretKeyRef.AWS_SECRET_ACCESS_KEY=s3-credentials:secret-key \
  --conf spark.kubernetes.executor.secretKeyRef.AWS_ACCESS_KEY_ID=s3-credentials:access-key \
  --conf spark.kubernetes.executor.secretKeyRef.AWS_SECRET_ACCESS_KEY=s3-credentials:secret-key \
  --conf spark.hadoop.fs.s3a.endpoint=https://rgw.example.internal \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  s3a://jobs/tripatha.spark-1.0.0-SNAPSHOT-job.jar \
  smoke --table-prefix t500m_ --accumulo-properties /etc/tripatha/accumulo-client.properties \
        --work-dir s3a://accumulo/tripatha-bulk/smoke
```

**Then the load.** For the loader's dataset at 500M triples, prepare the store first with the loader, then run the same dataset options in the job. To measure the cluster afterwards with the join benchmark, load the benchmark's own dataset instead (`--text-index --geo-index`, and `--entity-index`): see [Measuring on a cluster](../tripatha.loader/README.md#measuring-on-a-cluster).

```bash
tripatha.loader/run.sh prepare --table-prefix t500m_ --accumulo-properties accumulo-client.properties --triples 500M

spark-submit ...the same --conf as above... \
  --conf spark.executor.instances=16 --conf spark.executor.cores=4 --conf spark.executor.memory=12g \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.claimName=OnDemand \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.storageClass=YOUR_STORAGE_CLASS \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.sizeLimit=100Gi \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.mount.path=/data \
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.mount.readOnly=false \
  s3a://jobs/tripatha.spark-1.0.0-SNAPSHOT-job.jar \
  generate --table-prefix t500m_ --accumulo-properties /etc/tripatha/accumulo-client.properties --triples 500M \
           --work-dir s3a://accumulo/tripatha-bulk/t500m
```

For RDF files, a new store is created and split by the job itself:

```bash
spark-submit ... s3a://jobs/tripatha.spark-1.0.0-SNAPSHOT-job.jar \
  files --table-prefix dump_ --accumulo-properties /etc/tripatha/accumulo-client.properties \
        --input s3a://datasets/dump/ --work-dir s3a://accumulo/tripatha-bulk/dump --text-index
```

The endpoint then uses the same table prefix and index settings. Delete the work directory once the job prints `Import: … can be deleted`.

Notes:
- **Compressed input:** a `.gz` file can't be split, so one task reads it whole. Many files, or uncompressed or `.bz2` ones, read in parallel.
- **Format:** N-Triples and N-Quads are read line by line. Convert Turtle, RDF/XML or JSON-LD to N-Quads first (for instance with `riot` or `rapper`).
- **Labels:** N-Quads has no place for a visibility label, so `--visibility EXPR` gives every statement of the job the same one.
- **Sizes:** `--file-size` (MB of cells before compression, 1024 by default) sets the size of a file and of a reduce task. A smaller size makes more, shorter reduce tasks, and more files per tablet (at most the table's `table.file.max`). The shuffle moves a little more than the 3.5 cells per statement the files hold (counters not yet added up across tasks), roughly 100 bytes each before Spark compresses them, and spills them to the executors' local disks.
- **Executors:** 4 cores and 12 GB each is a reasonable start. A task holds its counters to add up (about 80 MB) besides Spark's sort buffers.

## Run on Kubernetes without a Spark image

Where Accumulo stores its files on a shared volume (CephFS, NFS) and no Spark image runs Java 25, `k8s/` runs the job on stock `eclipse-temurin:25-jre` pods:
- **Spark** and the tools sit on Accumulo's own volume, under `DATA_PATH/tripatha-tools`.
- **A standalone Spark cluster** (`spark-standalone.yaml`) runs one master and one worker per node. Each worker has node-local disk for the shuffle and mounts the volume.
- **A driver pod** (`driver-pod.yaml`) runs the loader, the benchmark or the Spark job in client mode, with Accumulo's password from a secret.

Every object is labelled `app=tripatha`: `kubectl -n NS delete all,pod -l app=tripatha` removes them. The variables, for Accumulo in namespace `accumulo` with its volume `accumulo-data` at `/var/accumulo/data`:

```bash
export NAMESPACE=accumulo DATA_PVC=accumulo-data DATA_PATH=/var/accumulo/data
export WORKERS=4 WORKER_CORES=2 WORKER_MEMORY=3500m WORKER_POD_MEMORY=4608Mi WORKER_DISK=55Gi AVOID_NODES=none
export ACCUMULO_INSTANCE=accumulo ACCUMULO_ZOOKEEPERS=accumulo-zookeeper-server.accumulo.svc.cluster.local:2282 ACCUMULO_USER=root
export SECRET=accumulo-secret SECRET_KEY=root-password DRIVER_MEMORY=2Gi DRIVER_HEAP=1280m EXECUTOR_MEMORY=3g
V='${NAMESPACE} ${DATA_PVC} ${DATA_PATH} ${WORKERS} ${WORKER_CORES} ${WORKER_MEMORY} ${WORKER_POD_MEMORY} ${WORKER_DISK} ${AVOID_NODES} ${NAME} ${ACCUMULO_INSTANCE} ${ACCUMULO_ZOOKEEPERS} ${ACCUMULO_USER} ${SECRET} ${SECRET_KEY} ${DRIVER_MEMORY} ${DRIVER_HEAP} ${EXECUTOR_MEMORY} ${DRIVER_COMMAND}'
K=tripatha.spark/k8s
```

`envsubst "$V"` substitutes only these, so the pods' own `$POD_IP` and `$A` stay. Then:

```bash
# 1. The tools (the job jar, the loader, the benchmark) and Spark 4.2.1, checked, onto the volume
$K/pack-tools.sh /tmp/tripatha-tools.tar.gz
curl -fLO https://dlcdn.apache.org/spark/spark-4.2.1/spark-4.2.1-bin-hadoop3.tgz
curl -fsL https://archive.apache.org/dist/spark/spark-4.2.1/spark-4.2.1-bin-hadoop3.tgz.sha512 | shasum -a 512 -c -
envsubst "$V" < $K/stage-pod.yaml | kubectl apply -f -
kubectl -n $NAMESPACE wait --for=condition=Ready pod/tripatha-stage
kubectl -n $NAMESPACE cp spark-4.2.1-bin-hadoop3.tgz tripatha-stage:$DATA_PATH/tripatha-tools/
kubectl -n $NAMESPACE cp /tmp/tripatha-tools.tar.gz tripatha-stage:$DATA_PATH/tripatha-tools/
kubectl -n $NAMESPACE exec tripatha-stage -- bash -c "cd $DATA_PATH/tripatha-tools && tar xzf spark-4.2.1-bin-hadoop3.tgz && tar xzf tripatha-tools.tar.gz && rm *.tgz *.tar.gz"
kubectl -n $NAMESPACE delete pod tripatha-stage

# 2. The Spark cluster: check that the master's log shows one "Registering worker" per node
envsubst "$V" < $K/spark-standalone.yaml | kubectl apply -f -

# 3. A driver pod per step: here the cluster check, then prepare and generate at 500M
NAME=tripatha-smoke DRIVER_COMMAND='submit smoke --table-prefix t_ --accumulo-properties $A --work-dir file://'$DATA_PATH'/tripatha-bulk/smoke' \
  envsubst "$V" < $K/driver-pod.yaml | kubectl apply -f -
kubectl -n $NAMESPACE logs -f tripatha-smoke
DS="--table-prefix b500m_ --triples 500M --text-index --geo-index --entity-index"
NAME=tripatha-load DRIVER_COMMAND="loader prepare --accumulo-properties \$A $DS && submit generate --accumulo-properties \$A $DS --work-dir file://$DATA_PATH/tripatha-bulk/b500m --tasks 256 --file-size 512" \
  envsubst "$V" < $K/driver-pod.yaml | kubectl apply -f -
```

- **Leave the cluster room:** each worker is limited to `WORKER_CORES`. Where Ceph or ZooKeeper run on the same nodes, a Spark job that takes every idle core starves them. On a cluster of four 8-core nodes, with 4 cores per worker and no limit, ZooKeeper's fsyncs reached 7.5 s within 20 minutes, a ZooKeeper session expired, and Accumulo's compaction coordinator crashed. With 2 cores per worker the stalls went on there: they came from one slow Ceph OSD (`BLUESTORE_SLOW_OP_ALERT`), whose commit latency rose from about 10 ms to 12–19 s under write bursts while the others stayed under 15 ms. Check `ceph health detail` and `ceph osd perf` before a large load, and keep workers off a node whose OSD is slow: `AVOID_NODES=sleepy.example.local` (comma-separated host names; `none` avoids none), with `WORKERS` one per remaining node. Watch ZooKeeper's log for `fsync-ing the write ahead log … took` during a load. Keep ZooKeeper's data off Ceph, on node-local volumes: on RBD its fsyncs share the OSDs and the network with the job's writes, and stalled for seconds on 1 GbE. Keep the shuffle off nodes whose disk also serves an OSD or a ZooKeeper server it slows down: on that cluster, only the node without etcd and a Ceph monitor took the shuffle without slowing either. The driver pod gives each executor a whole worker (`spark.executor.cores` = `WORKER_CORES`).
- **A CPU limit above the task slots:** besides its tasks, an executor's JVM runs GC, the JIT and the shuffle's network and compression threads. With the limit equal to 2 slots the workers were throttled in 78–87 % of CFS periods while their nodes had cores idle; a limit of 3 for the same 2 slots made a 100M part 15 % faster (the reduce stage 20 %). `spark-standalone.yaml` sets the limit to `WORKER_CORES` (O29 will take it apart); to raise it, edit the worker's `limits` line.
- **Sizing:** workers and drivers must fit the nodes' free memory requests (`kubectl describe nodes`), which on a shared cluster can be much less than the nodes' memory. The figures above fit nodes of 32 GB that run Accumulo and other services: 4.5 GiB per worker (an executor of 3 GB and 2 cores), 2 GiB for the driver.
- **Executors that don't spill:** a task gets about (executor heap − 300 MB) × 0.6 / slots of execution memory, and a map task's output takes about 1.6 KB of it per statement. Past that, the task spills its output to the local disk and merges it back, which doubles the writes the disks see. With 3 GB executors of 2 slots and 64 map tasks per 100M triples (1.56M statements each), every record spilled once in the map stage and 13 GB more in the reduce: 33.6 GB of spills beside a 20.6 GB shuffle. With 5 GB executors (`WORKER_MEMORY=5500m WORKER_POD_MEMORY=7Gi EXECUTOR_MEMORY=5g`) and `--tasks 256` per 100M triples (390,000 statements a task, 620 MB at its peak; 950 MB per reduce task at `--file-size 512`) the spills fell to 1.2 GB, the part ran 16 % faster, and ZooKeeper stalled a quarter as often.
- **Loads in parts:** each part's import adds a file to most tablets, and with Accumulo's default `table.compaction.major.ratio` of 3 a tablet of four similar files is compacted. After the fourth part of 500M that queued about 140 major compactions, and running beside the next part they stalled a ZooKeeper fsync for 54 s. Hold them for the load and compact once at the end: `config -t PREFIXspo -s table.compaction.major.ratio=100` on the large tables (`spo`, `po`, `osp`, `eval`, `text`, `entity`) before the parts, and `config -t PREFIXspo -d table.compaction.major.ratio` after the last.
- **Measuring a run:** a `conf/spark-defaults.conf` in the staged Spark directory reaches every driver pod's `spark-submit`. `spark.eventLog.enabled true` with `spark.eventLog.dir file://DATA_PATH/tripatha-bulk/events` (and `spark.eventLog.compress false`) records each task's run, CPU, GC, shuffle and spill figures. `spark.executor.extraJavaOptions -XX:FlightRecorderOptions=repository=DIR/{{APP_ID}}-exec{{EXECUTOR_ID}} -XX:StartFlightRecording=maxsize=400m,dumponexit=true,filename=DIR/{{APP_ID}}-exec{{EXECUTOR_ID}}.jfr` gives each executor a JFR recording of its own: in pods every executor has the same pid, so JFR's default names collide. gz and zstd run in native code, which JFR samples apart (`jdk.NativeMethodSample`).
- **Shuffle disk:** at 10M triples with every index the job shuffled 3.5 GB (92M records, lz4). At 500M on four workers, plan for 30–45 GB of local disk per node (with zstd: 13 GB per node after the first 16 minutes of the map stage). Each worker's `emptyDir` must hold its share (a third more on three workers); a node out of disk evicts the pod.
- **The driver** runs in client mode: the executors connect to it on its pod address (ports 7078 and 7079).

## Measured

At 10M triples on one machine (12 cores, Spark in local mode on 10, the Accumulo test cluster), 2026-11-21:

| | Time |
|---|---|
| The loader's `build`, 10 threads | 50.8 s |
| `generate` | 63.5 s: map 13.6, reduce 43.8, import 1.5 |
| `files`: WatDiv sf100, 10,950,947 triples in 1.55 GB of N-Triples | 52.3 s: sample 0.7, map 14.2, reduce 28.5 |

On one machine the job costs 1.25 times the loader's build. On a cluster both stages spread over every executor. Details in [SCALE-STUDY.md](../SCALE-STUDY.md) section 4.28.

On the user's cluster (four RK3588 nodes, 1 GbE, Accumulo on CephFS), 500M triples with every index as five `generate` parts of 100M, four workers of 2 task slots, 2026-11-23:

| Part | Sizing | Time | Map | Reduce |
|---|---|---|---|---|
| 2 | 3 GB executors, `--tasks 64`, CPU limit 2 | 35:37 | 701 s | 1,425 s |
| 3 | 5 GB executors, `--tasks 256` | 29:59 | 608 s | 1,181 s |
| 4 | and a CPU limit of 3 | 25:31 | 580 s | 940 s |

Each part writes about 790M cells in 296 files; the import takes 14–35 s.

## Try it on one machine

`LocalCluster` (test sources) starts a throwaway Accumulo whose volume is the local disk, and writes its client properties:

```bash
mvn -pl tripatha.spark -am install -DskipTests -DskipITs
mvn -pl tripatha.spark dependency:build-classpath -Dmdep.includeScope=test -Dmdep.outputFile=/tmp/cp.txt
java -cp tripatha.spark/target/test-classes:$(cat /tmp/cp.txt) org.tripatha.spark.LocalCluster /tmp/acc &

spark-submit --master 'local[8]' --class org.tripatha.spark.SparkBuild tripatha.spark/target/tripatha.spark-1.0.0-SNAPSHOT-job.jar \
  smoke --table-prefix try_ --accumulo-properties /tmp/acc/accumulo-client.properties --work-dir /tmp/acc-work
```

## Tests

`SparkBuildIT` runs the job on local Spark against the test cluster (Accumulo only).
- **`generate`** writes every cell the loader's own build writes, table by table: statements, every index, counters, text counts and graph markers.
- **`files`** writes every cell a DAO load of the same statements writes. It covers blank nodes across splits and across files, graphs, triple terms, a base direction, a label, a repeated statement, a bad line, `--distinct`, a write run again after a failure, and the refusal of a store that holds statements.
- **`smoke`** passes, and drops its table.

`SparkJava25SpikeTest` is the first check: Spark 4.2's tasks run Tripatha's encoding on Java 25.
