# FoxTrot live-resharding latency experiment

Goal: measure whether one FoxTrot `3 -> 4` active-master scale-up temporarily
increases NoSQLMark read p95. FoxTrot transfers half one donor's slots to an
existing standby, then provisions the next standby. Save data for later
Pre / Reshard / Post read/update p95 bars and a one-second latency timeline,
following the [Mambo experiment](../../NoSQLMark-experiments/docs/experiment_overview_rebalancing_latency.md).

Use the same Kubernetes cluster, with a **fresh experiment namespace per run**.
This leaves the existing `foxtrot` and Mambo deployments/data intact. Run one
pilot first; use three fresh runs for repetition. A temporary latency penalty
alone does not establish that scaling was uneconomic.

## Fixed design and safety

| Item | Setting |
|---|---|
| Redis | 7.2; 3 active masters, 1 replica each, plus an empty standby and its replica |
| Redis resources | 0.5/1 CPU request/limit; 512 MiB/2 GiB memory; 16 GiB PVCs |
| Data | 1,000,000 records; ten 100-byte fields; hashed insertion order |
| Workload | Uniform 80% reads / 20% updates; read/write all fields |
| NoSQLMark | Constant open-loop arrivals; one backend, one pacing worker |
| Offered rate | Calibrate from 500 RPS; freeze the chosen clean rate |
| Pilot | 60 seconds warm-up + 480 measured seconds; enable scaling after 90 measured seconds |
| Scaling | CPU trigger; memory below 70%; cooldown 3600 seconds |

Follow [AGENTS.md](../AGENTS.md). `[READ ONLY]` inspects state, `[LOCAL CHANGE]`
writes artifacts, and `[CLUSTER CHANGE]`, `[POD CHANGE]`, or `[DATABASE CHANGE]`
identify mutations for the user to execute or specifically authorize. None of
these instructions authorize Codex to run mutations merely by writing the guide.
Section 10 retires only the completed run's experiment namespace after results
are preserved. No cluster reset or shared monitoring/controller replacement is included.
Do not run other benchmark traffic concurrently on the experiment's nodes.

Use four terminals: **Main**, **Prometheus**, **Backend**, and **REPL**. Run the
sections in order, following their terminal labels. Wait for each NoSQLMark job's
result/export before submitting another. The preparation/build steps use the
Redis Cluster binding in the sibling `NoSQLMark-experiments` repository.
Stop at an unexpected command/check failure instead of continuing into the next
step. `NotFound` is expected only for the absent-operator preflight.
Local prerequisites: Bash, kubectl, jq, curl, tar, Python 3.9+, and
access to the existing cluster/default StorageClass. JVM timestamps are explicitly
UTC; this pilot assumes the existing node time synchronization is working.

## 1. Select the cluster and create an isolated artifact directory

**Main** — `[LOCAL CHANGE; READ ONLY]`:

```bash
cd /users/adas2125/foxtrot-analysis
export EXP_HOME="$PWD"
export KCFG=/users/adas2125/.kube/amit.kubeconfig
export CTX="$(kubectl --kubeconfig "$KCFG" config current-context)"
export RUN_ID="$(date -u +%Y%m%d-%H%M%S)"
export NS="foxtrot-rebalance-$RUN_ID"
export CLUSTER="redis-$RUN_ID"
export CLIENT=nosqlmark-client
export CLIENT_NODE=i-063b793db694be24c
export RECORD_COUNT=1000000
export ARTIFACT_ROOT="$EXP_HOME/rebalancing-latency-experiment-artifacts"
export RUN_DIR="$ARTIFACT_ROOT/$RUN_ID"
mkdir -p "$ARTIFACT_ROOT"
mkdir "$RUN_DIR"
mkdir "$RUN_DIR/manifests" "$RUN_DIR/nosqlmark" "$RUN_DIR/calibration"
kc() { kubectl --kubeconfig "$KCFG" --context "$CTX" "$@"; }
k() { kc -n "$NS" "$@"; }
{
  declare -p EXP_HOME KCFG CTX RUN_ID NS CLUSTER CLIENT CLIENT_NODE RECORD_COUNT
  declare -p ARTIFACT_ROOT RUN_DIR
  declare -f kc k
} > "$RUN_DIR/run-env.sh"
printf 'Context: %s\nNamespace: %s\nsource %q\n' "$CTX" "$NS" "$RUN_DIR/run-env.sh"
kc get nodes -o wide
kc get storageclass
kc get deployment redis-operator-controller-manager -n redis-operator-system
kc get redisclusters.cache.example.com -A
```

Confirm the context/client node and a working default StorageClass. A new run
must use an absent namespace; do not apply these fresh-setup commands over an
existing experiment. In every other terminal, run the exact `source` command
printed above. If Main appends new variables later, source it again there as directed.

The existing FoxTrot operator should be reused. **Only if the operator and its
CRD are absent**, install the local repository's manifest `[CLUSTER CHANGE]`:

```bash
kc apply -f "$EXP_HOME/../redis-foxtrot-autoscaler/operator.yaml"
kc rollout status -n redis-operator-system \
  deployment/redis-operator-controller-manager --timeout=300s
```

Do not reinstall or replace an existing shared controller. If its version/behavior
differs from the local implementation, resolve that before running this experiment.

## 2. Prepare Redis and the client pod

The namespace LimitRange supplies the resource defaults missing from FoxTrot's
Redis container template. Unlike patching the StatefulSet, this does not fight
operator reconciliation. Explicit exporter/client resources take precedence.
See Kubernetes [LimitRange defaults](https://kubernetes.io/docs/concepts/policy/limit-range/).
The existing [namespace manifest](../manifests/foxtrot-namespace.yaml) targets
`foxtrot` and sets 250m/256 MiB requests and 1 CPU/1 GiB limits. This experiment
uses a fresh namespace and the larger resource settings in the fixed design above,
so create its namespace and LimitRange directly below; no generated namespace
manifest is needed. Keep the existing manifest unchanged.

Precreate the ten PVCs needed through one scale-up: FoxTrot otherwise requests
only 1 GiB, which is tight for this dataset plus append-only files. Keep Redis's
existing AOF behavior unchanged. Stream the PVC definitions directly to
`kubectl create` below instead of saving `pvcs.yaml`; the 16 GiB claims themselves
are still required for this design.

**Main** — `[LOCAL CHANGE]`:

```bash
cat > "$RUN_DIR/manifests/client.yaml" <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $CLIENT
  namespace: $NS
spec:
  restartPolicy: Never
  nodeSelector:
    kubernetes.io/hostname: $CLIENT_NODE
  affinity:
    podAntiAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
      - labelSelector:
          matchLabels: {cluster: "$CLUSTER", component: redis}
        topologyKey: kubernetes.io/hostname
  containers:
  - name: client
    image: maven:3.8.7-eclipse-temurin-8
    command: [sleep, infinity]
    resources:
      requests: {cpu: "1", memory: 2Gi}
      limits: {cpu: "2", memory: 4Gi}
EOF

cat > "$RUN_DIR/manifests/redis.yaml" <<EOF
apiVersion: cache.example.com/v1
kind: RedisCluster
metadata:
  name: $CLUSTER
  namespace: $NS
spec:
  masters: 3
  minMasters: 3
  replicasPerMaster: 1
  redisVersion: "7.2"
  existingCluster: false
  manageStatefulSet: true
  autoScaleEnabled: false
  cpuThreshold: 60
  cpuThresholdLow: 1
  memoryThreshold: 70
  memoryThresholdLow: 30
  reshardTimeoutSeconds: 600
  scaleCooldownSeconds: 3600
  prometheusURL: http://prometheus-operated.monitoring.svc:9090
  metricsQueryInterval: 15
EOF
```

**Main** — `[CLUSTER CHANGE]`. Create the fresh namespace, then start the client
before Redis; its required anti-affinity keeps this experiment's Redis pods off
the client node. The subshell stops on a failure, including an existing namespace,
before continuing to later setup commands.

```bash
(
set -euo pipefail
kc create namespace "$NS"
k create -f - <<EOF
apiVersion: v1
kind: LimitRange
metadata:
  name: redis-defaults
  namespace: $NS
spec:
  limits:
  - type: Container
    defaultRequest: {cpu: 500m, memory: 512Mi}
    default: {cpu: "1", memory: 2Gi}
EOF

k create -f "$RUN_DIR/manifests/client.yaml"
k wait --for=condition=Ready "pod/$CLIENT" --timeout=300s
for index in $(seq 0 9); do
  cat <<EOF
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data-$CLUSTER-$index
  namespace: $NS
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests: {storage: 16Gi}
EOF
done | k create -f -

k create -f "$RUN_DIR/manifests/redis.yaml"
k wait --for=jsonpath='{.status.initialized}'=true \
  "rediscluster/$CLUSTER" --timeout=600s
k get pods,pvc -o wide
k exec "$CLUSTER-0" -c redis -- redis-cli cluster info
k exec "$CLUSTER-0" -c redis -- redis-cli cluster nodes
k get pods -l "cluster=$CLUSTER,component=redis" -o json \
  | jq '[.items[] | {pod:.metadata.name,node:.spec.nodeName,
      resources:[.spec.containers[] | {name,resources}]}]'
)
```

Expected: eight Redis pods, three slot-owning masters plus one empty standby,
healthy replicas, `cluster_state:ok`, 16,384 assigned slots, no failed or open
slots, and Redis limits of 1 CPU/2 GiB. PVCs 8 and 9 may remain Pending until their
standby pods are created. If pods cannot schedule or storage cannot bind, resolve
capacity/placement before loading; do not relax isolation during measurement.

## 3. Connect to Prometheus

**Prometheus** — `[READ ONLY: TEMPORARY CONNECTION]`; leave running:

```bash
kc -n monitoring port-forward service/kps-kube-prometheus-stack-prometheus 19090:9090
```

**Main** — `[READ ONLY; LOCAL CHANGE]`:

```bash
curl -fsS http://127.0.0.1:19090/-/ready
kc -n monitoring get prometheus kps-kube-prometheus-stack-prometheus \
  -o jsonpath='{.spec.serviceMonitorSelector}{"\n"}{.spec.serviceMonitorNamespaceSelector}{"\n"}'
export FT_CPU="100 * rate(container_cpu_usage_seconds_total{namespace=\"$NS\",pod=~\"$CLUSTER-.*\",container=\"redis\",service=\"kps-kube-prometheus-stack-kubelet\"}[1m]) and on(pod) redis_instance_info{namespace=\"$NS\",role=\"master\"}"
export FT_MEMORY="100 * sum by(pod) (container_memory_usage_bytes{namespace=\"$NS\",pod=~\"$CLUSTER-.*\",container=\"redis\"}) / sum by(pod) (kube_pod_container_resource_limits{namespace=\"$NS\",pod=~\"$CLUSTER-.*\",resource=\"memory\"}) and on(pod) redis_instance_info{namespace=\"$NS\",role=\"master\"}"
export CPU_CORES="sum by(pod) (rate(container_cpu_usage_seconds_total{namespace=\"$NS\",pod=~\"$CLUSTER-[0-9]+|$CLIENT\",container!=\"\",container!=\"POD\",container!=\"redis-exporter\",service=\"kps-kube-prometheus-stack-kubelet\"}[1m]))"
prom() {
  curl -fsSG http://127.0.0.1:19090/api/v1/query --data-urlencode "query=$1"
}
{
  declare -p FT_CPU FT_MEMORY CPU_CORES
  declare -f prom
} >> "$RUN_DIR/run-env.sh"
prom "$FT_CPU" | jq '.data.result'
prom "$FT_MEMORY" | jq '.data.result'
```

In the reference cluster both selectors are `{}`; FoxTrot creates its own
ServiceMonitor. Wait at least a minute for scrapes. Both queries must return
finite values for all four masters, including the empty standby. If monitoring
selection differs or a query stays empty, stop and resolve selection for the
experiment's ServiceMonitor; do not change shared Prometheus blindly. FoxTrot's
CPU is percent of one core, not percent of its CPU request. The memory query
above intentionally matches the operator's pod-limit denominator.

## 4. Build NoSQLMark with a Redis Cluster binding

The [RedisClusterClient.java](../../NoSQLMark-experiments/redis_binding/src/main/java/de/unihamburg/informatik/nosqlmark/db/RedisClusterClient.java)
binding is part of the NoSQLMark source. The copy below includes it, and the
backend build compiles it through its existing `redis_binding` dependency.
It uses Jedis 2.9.0 to route hash operations across masters and handle cluster
redirects. Load, reads, and updates use `usertable:user<hashed-record-number>`
keys; scans/deletes are unsupported. Pool waiting and retries remain within the
measured operation.

**Main** — `[POD CHANGE]`: copy source into the fresh pod, excluding old artifacts
and builds. The MongoDB dependency is also built because NoSQLMark declares it.

```bash
k exec "$CLIENT" -- mkdir -p /NoSQLMark
tar -C "$EXP_HOME/../NoSQLMark-experiments" \
  --exclude=.git --exclude=target --exclude=logs --exclude=results \
  --exclude=artifacts --exclude='*-artifacts' --exclude='*-plots' -cf - . \
  | k exec -i "$CLIENT" -- tar -xf - -C /NoSQLMark
k exec "$CLIENT" -- bash -lc '
  set -euo pipefail
  git clone https://github.com/steffenfriedrich/YCSB.git /YCSB
  git -C /YCSB checkout --detach b73ac8367b7de0356031684883338ec1826c1a4f
  sed -i "s#http://www.allanbank.com/repo/#https://www.allanbank.com/repo/#" /YCSB/mongodb/pom.xml
  sed -i "s#<mongodb.version>3.0.3</mongodb.version>#<mongodb.version>3.12.14</mongodb.version>#" /YCSB/pom.xml
  cd /YCSB
  mvn -pl mongodb -am -DskipTests -Dcheckstyle.skip=true install
  cd /NoSQLMark
  mkdir -p artifacts logs results backbench/logs
  curl -fL https://repo.scala-sbt.org/scalasbt/ivy-releases/org.scala-sbt/sbt-launch/0.13.8/sbt-launch.jar \
    -o artifacts/sbt-launch-0.13.8.jar
  echo "6570bb03df6138ffaa7ac0bbe35eb4ea79062d1146b6929c75cf238d14dd9158  artifacts/sbt-launch-0.13.8.jar" | sha256sum -c -
  printf "\nblocking-io-dispatcher.thread-pool-executor.fixed-pool-size = 64\n" >> config/nosqlmark.conf
'
```

**Main** — `[POD CHANGE]`: compile and save classpaths once. Direct Java startup
below avoids keeping sbt JVMs resident during measurement.

```bash
k exec "$CLIENT" -- bash -lc '
  set -euo pipefail
  cd /NoSQLMark
  java -Xmx1G -jar artifacts/sbt-launch-0.13.8.jar "project backbench" compile "project repl" compile
  java -Xmx1G -jar artifacts/sbt-launch-0.13.8.jar "project backbench" "export fullClasspath" \
    | tail -n 1 > artifacts/backend.cp
  java -Xmx1G -jar artifacts/sbt-launch-0.13.8.jar "project repl" "export fullClasspath" \
    | tail -n 1 > artifacts/repl.cp
'
```

Load and calibration provide the cluster smoke check; fix failed reads,
failed writes, or missing counts before the pilot.

## 5. Define jobs and start Backend/REPL

**Main** — `[LOCAL CHANGE; POD CHANGE]`: the same key encoding and fields are
used for load, calibration, and measurement.

```bash
cat > "$RUN_DIR/jobs.scala" <<EOF
val records = $RECORD_COUNT
val runID = "$RUN_ID"
val base = CoreJob(
  dbname = "de.unihamburg.informatik.nosqlmark.db.RedisClusterClient",
  dbproperties = Map(
    "redis.seeds" -> "$CLUSTER-0.$CLUSTER-headless.$NS.svc.cluster.local:6379,$CLUSTER-1.$CLUSTER-headless.$NS.svc.cluster.local:6379",
    "redis.pool.max" -> "100"
  ),
  workload = "CoreWorkload", table = "usertable", phase = "transactional",
  target = 500.0, nodes = 1, worker = 1, asyncmode = true,
  counts = CoreCounts(recordcount = records, warmupcount = 0,
    operationcount = 1, insertcount = 0, insertstart = 0,
    fieldcount = 10, fieldlength = 100, readallfields = true, writeallfields = true),
  proportions = CoreProportions(readproportion = 0.8, updateproportion = 0.2,
    insertproportion = 0.0, scanproportion = 0.0, readmodifywriteproportion = 0.0),
  distributions = CoreDistributions(requestdistribution = "uniform", insertorder = "hashed"),
  loadgeneration = CoreLoadGeneration(interrequesttimedistribution = "constant"),
  logmeasurements = false, logjvmstats = false
)
val loadJob = base.copy(jobID = nc.genID, batchname = "load-" + runID,
  phase = "load", target = 2000.0,
  counts = base.counts.copy(operationcount = records, insertcount = records))
def calibration(rate: Int) = base.copy(jobID = nc.genID,
  batchname = "calibration-" + runID + "-" + rate, target = rate.toDouble,
  counts = base.counts.copy(warmupcount = 30 * rate, operationcount = 90 * rate))
def pilot = {
  val rate = new String(java.nio.file.Files.readAllBytes(
    java.nio.file.Paths.get("/NoSQLMark/artifacts/pilot-rate")), "UTF-8").trim.toInt
  base.copy(jobID = nc.genID, batchname = "foxtrot-rebalancing-" + runID,
    target = rate.toDouble, logmeasurements = true,
    counts = base.counts.copy(warmupcount = 60 * rate, operationcount = 480 * rate))
}
EOF
k cp "$RUN_DIR/jobs.scala" "$CLIENT:/NoSQLMark/artifacts/jobs.scala"
```

**Backend** — `[POD CHANGE]`, after sourcing the run environment:

```bash
k exec -it "$CLIENT" -- bash -lc '
  cd /NoSQLMark/backbench
  exec java -Xmx2G -Duser.timezone=UTC \
    -Dlogback.configurationFile=/NoSQLMark/config/logback.xml \
    -cp "$(cat /NoSQLMark/artifacts/backend.cp)" \
    de.unihamburg.informatik.nosqlmark.BackBench
'
```

**REPL** — `[POD CHANGE]`:

```bash
k exec -it "$CLIENT" -- bash -lc '
  cd /NoSQLMark
  exec java -Xmx1G -Duser.timezone=UTC \
    -Dlogback.configurationFile=/NoSQLMark/config/logback.xml \
    -cp "$(cat /NoSQLMark/artifacts/repl.cp)" \
    de.unihamburg.informatik.nosqlmark.repl.REPL
'
```

Wait for `Connected to BackbenchService`, then enter in REPL:

```scala
:load /NoSQLMark/artifacts/jobs.scala
```

Define these two small collection helpers in **Main** `[LOCAL CHANGE]`. They
copy only summary/workload JSON, and check operation counts and failures.

```bash
save_result() {
  local batch="$1" destination="$2" remote_dir
  remote_dir="$(k exec "$CLIENT" -- env BATCH="$batch" bash -lc '
    set -euo pipefail
    mapfile -t files < <(find "/NoSQLMark/results/$BATCH" -name summary.json)
    test "${#files[@]}" -eq 1
    test -f "$(dirname "${files[0]}")/workload.json"
    dirname "${files[0]}"
  ')" || return 1
  mkdir "$destination" || return 1
  k cp "$CLIENT:$remote_dir/summary.json" "$destination/summary.json" || return 1
  k cp "$CLIENT:$remote_dir/workload.json" "$destination/workload.json"
}
check_summary() {
  jq -e --argjson expected "$2" '
    (.ALL.Count | tonumber) == $expected and
    ([to_entries[] | select(.key | test("-(FAILED|TIMEDOUT)$")) |
      (.value.Count | tonumber)] | add // 0) == 0
  ' "$1/summary.json"
}
{
  declare -f save_result check_summary
} >> "$RUN_DIR/run-env.sh"
```

## 6. Load and verify the dataset

**Main** — `[READ ONLY]`: verify the fresh database is empty. `DBSIZE` is queried
only on masters, so replicas do not double-count records. Use this helper before
and after load, and once after the measured run; never scan keys during traffic.

```bash
master_counts() {
  local pod
  for pod in $(k get pods -l "cluster=$CLUSTER,component=redis" -o json | \
      jq -r '.items[] | select(.metadata.ownerReferences[]?.kind == "StatefulSet") | .metadata.name'); do
    k exec "$pod" -c redis -- sh -c '
      if [ "$(redis-cli --raw role | head -n 1)" = master ]; then
        printf "%s " "$HOSTNAME"
        redis-cli --raw dbsize
      fi
    ' || return 1
  done
}
declare -f master_counts >> "$RUN_DIR/run-env.sh"
master_counts | tee "$RUN_DIR/counts-empty.txt"
awk '{n += $2} END {exit n != 0}' "$RUN_DIR/counts-empty.txt"
```

**REPL** — `[DATABASE CHANGE]`: run once, then wait for its result/export:

```scala
nc.submitJob(loadJob)
```

**Main** — `[READ ONLY; LOCAL CHANGE]`:

```bash
save_result "load-$RUN_ID" "$RUN_DIR/load"
check_summary "$RUN_DIR/load" "$RECORD_COUNT"
master_counts | tee "$RUN_DIR/counts-loaded.txt"
awk -v expected="$RECORD_COUNT" '{n += $2} END {exit n != expected}' "$RUN_DIR/counts-loaded.txt"
k exec "$CLUSTER-0" -c redis -- redis-cli cluster nodes \
  > "$RUN_DIR/slots-before.txt"
sleep 120
prom "$FT_CPU" > "$RUN_DIR/calibration/idle-cpu.json"
```

Require three masters with data and an empty standby. A failed/partial load must
be diagnosed; do not blindly submit the load again over existing keys. The two-minute
settle lets the load leave FoxTrot's one-minute CPU window.

## 7. Calibrate rate and choose the CPU threshold

Autoscaling remains disabled throughout calibration. Start at **500 RPS** and,
if clean but too light, repeat at 1000, 2000, 4000, and so on. Each candidate runs
30 seconds warm-up + 90 seconds measurement. Run each candidate rate once; keep
its summary/workload and metric snapshots for manual review.

**Main** — `[READ ONLY; LOCAL CHANGE]`: set the candidate before submitting it:

```bash
export CAL_RPS=500
export STANDBY="$(k get rediscluster "$CLUSTER" -o jsonpath='{.status.standbyPod}')"
printf 'Exclude standby from CPU selection: %s\n' "$STANDBY"
```

**REPL** — `[DATABASE CHANGE]` (use the same rate as `CAL_RPS`):

```scala
nc.submitJob(calibration(500))
```

**Main**, about 90 seconds after submission, while REPL still reports traffic —
`[READ ONLY METRICS; LOCAL CHANGE]`. Repeat while traffic is running if readings
fluctuate; timestamped filenames preserve each capture. Filenames distinguish
CPU percent, memory percent, and CPU cores in the output:

```bash
CAL_SNAPSHOT="$(date -u +%Y%m%dT%H%M%S)"
prom "$FT_CPU" > "$RUN_DIR/calibration/cpu-$CAL_RPS-$CAL_SNAPSHOT.json"
prom "$FT_MEMORY" > "$RUN_DIR/calibration/memory-$CAL_RPS-$CAL_SNAPSHOT.json"
prom "$CPU_CORES" > "$RUN_DIR/calibration/cores-$CAL_RPS-$CAL_SNAPSHOT.json"
jq '{file: (input_filename | split("/")[-1]),
     values: [.data.result[] | {pod:.metric.pod,value:.value[1]}]}' \
  "$RUN_DIR/calibration/idle-cpu.json" \
  "$RUN_DIR/calibration/cpu-$CAL_RPS-$CAL_SNAPSHOT.json" \
  "$RUN_DIR/calibration/memory-$CAL_RPS-$CAL_SNAPSHOT.json" \
  "$RUN_DIR/calibration/cores-$CAL_RPS-$CAL_SNAPSHOT.json"
```

After the calibration result/export, in **Main**:

```bash
save_result "calibration-$RUN_ID-$CAL_RPS" "$RUN_DIR/calibration/$CAL_RPS"
check_summary "$RUN_DIR/calibration/$CAL_RPS" "$((90 * CAL_RPS))"
jq '.Overall' "$RUN_DIR/calibration/$CAL_RPS/summary.json"
```

Review manually: counts must match, failures/timeouts must be zero, throughput
must stay within 2% of the offered rate, client CPU below 1.5 cores, and there must
be no restarts/OOMs. Require finite metrics for all four masters, the busiest active
master **within 20–70% of one core** during steady traffic, and every master below
70% memory. Compare fluctuating readings; one brief peak above 20% is not enough.
Change rate only if CPU stays outside the range; address client saturation first.

Choose `CPU_THRESHOLD` halfway between the highest idle-master CPU and the typical
busiest-master CPU during calibration, excluding `$STANDBY`. Require at least a
5-percentage-point increase above idle. Round the midpoint down to an integer
above 1 and between idle and loaded CPU. For example, 0.2% idle and 20.9% loaded
give 10%. **Replace `10` below with the threshold chosen for this run.**

**Main** — `[LOCAL CHANGE]`:

```bash
export TARGET_RPS="$CAL_RPS"
export CPU_THRESHOLD=10
printf '%s\n' "$CPU_THRESHOLD" > "$RUN_DIR/cpu-threshold.txt"
declare -p TARGET_RPS CPU_THRESHOLD STANDBY >> "$RUN_DIR/run-env.sh"
printf '%s\n' "$TARGET_RPS" > "$RUN_DIR/pilot-rate"
```

**Main** — `[CLUSTER CHANGE; POD CHANGE]`: freeze the chosen policy and rate.

```bash
k patch rediscluster "$CLUSTER" --type merge --patch "$(
  jq -nc --argjson cpu "$CPU_THRESHOLD" \
    '{spec:{autoScaleEnabled:false,cpuThreshold:$cpu,scaleCooldownSeconds:3600}}'
)"
k cp "$RUN_DIR/pilot-rate" "$CLIENT:/NoSQLMark/artifacts/pilot-rate"
k get rediscluster "$CLUSTER" -o yaml > "$RUN_DIR/cluster-configured.yaml"
```

## 8. Start a fresh measured run and enable scaling

Stop **Backend** and **REPL** with `Ctrl-C` after the calibration export. Archive
the old client logs with both JVMs stopped, then restart them with the exact two
commands from section 5. This isolates the pilot's raw rows.

**Main** — `[POD CHANGE]`:

```bash
k exec "$CLIENT" -- env RUN_ID="$RUN_ID" bash -lc '
  set -euo pipefail
  ! ps -eo args | grep -E "nosqlmark[.]BackBench|nosqlmark[.]repl[.]REPL" | grep -v grep
  test ! -e "/NoSQLMark/backbench/logs-before-$RUN_ID"
  mv /NoSQLMark/backbench/logs "/NoSQLMark/backbench/logs-before-$RUN_ID"
  mkdir /NoSQLMark/backbench/logs
'
```

Restart Backend/REPL, wait for the connection, and reload in **REPL**:

```scala
:load /NoSQLMark/artifacts/jobs.scala
```

**Main** — `[READ ONLY; LOCAL CHANGE]`: capture the initial identities/restarts
and workload start. The same snapshot helper will be reused afterward.

```bash
pod_snapshot() {
  k get pods -o json | jq '[.items[] | select(.metadata.name == "nosqlmark-client" or
    (.metadata.ownerReferences // [] | any(.kind == "StatefulSet"))) |
    {pod:.metadata.name,uid:.metadata.uid,node:.spec.nodeName,
    containers:[.status.containerStatuses[]? | {name,imageID,restartCount}]}]'
}
pod_snapshot > "$RUN_DIR/pods-before.json"
kc -n redis-operator-system get pods -l control-plane=controller-manager \
  -o jsonpath='{range .items[*].status.containerStatuses[*]}{.imageID}{"\n"}{end}' \
  > "$RUN_DIR/operator-image.txt"
export START_EPOCH="$(date -u +%s)"
export START_UTC="$(date -u -d "@$START_EPOCH" +%Y-%m-%dT%H:%M:%SZ)"
{
  declare -f pod_snapshot
  declare -p START_EPOCH START_UTC
} >> "$RUN_DIR/run-env.sh"
```

**REPL** — `[DATABASE CHANGE]`:

```scala
nc.submitJob(pilot)
```

**Main** — `[READ ONLY]`: wait for the raw log to reach **90 measured seconds**.
Warm-up rows are absent. This checks actual measurement progress, not time since
submission. It times out if the client does not produce the expected baseline.

```bash
k exec "$CLIENT" -- bash -lc '
  for attempt in $(seq 1 150); do
    offset=$(tail -n 1 /NoSQLMark/backbench/logs/timeseries.log 2>/dev/null | cut -d\; -f2)
    if [ -n "$offset" ] && awk -v x="$offset" "BEGIN {exit !(x >= 90000)}"; then
      exit 0
    fi
    sleep 2
  done
  exit 1
'
```

Continue only if this succeeds and REPL still reports the chosen rate. Start the
log collector **before** enabling scaling: FoxTrot deletes the reshard Job after
completion. The collector runs in Main's background, so no fifth terminal is needed.

**Main** — `[READ ONLY; LOCAL CHANGE]`, then `[CLUSTER CHANGE]`:

```bash
(
  for attempt in $(seq 1 300); do
    if k get job "$CLUSTER-reshard" >/dev/null 2>&1; then
      if k logs -f "job/$CLUSTER-reshard" -c smart-reshard \
          --timestamps --pod-running-timeout=120s > "$RUN_DIR/reshard.log"; then
        exit 0
      fi

      # Preserve partial output if an established stream fails.
      if [ -s "$RUN_DIR/reshard.log" ]; then
        exit 1
      fi
    fi
    sleep 1
  done
  exit 1
) &
export CAPTURE_PID=$!
export ENABLE_EPOCH="$(date -u +%s)"
declare -p ENABLE_EPOCH >> "$RUN_DIR/run-env.sh"
k patch rediscluster "$CLUSTER" --type merge \
  --patch '{"spec":{"autoScaleEnabled":true}}'
```

Enabling permits the operator's full scale-up sequence: its consistency check,
slot migration, temporary full-coverage configuration changes, and next-standby
provisioning. Keep the offered rate unchanged for the full eight measured minutes.
There must be exactly one successful `3 -> 4` transition; the long cooldown blocks
another successful scale within this run. Failed/retried reshard attempts invalidate
the pilot. Do not run key scans or other benchmarks during measurement.

## 9. Collect the minimal result set and stop scaling

After **REPL** reports the result/export, in **Main** `[READ ONLY; LOCAL CHANGE]`:

```bash
export END_EPOCH="$(date -u +%s)"
save_result "foxtrot-rebalancing-$RUN_ID" "$RUN_DIR/nosqlmark/result"
check_summary "$RUN_DIR/nosqlmark/result" "$((480 * TARGET_RPS))"
mkdir "$RUN_DIR/nosqlmark/backend-logs"
k exec "$CLIENT" -- bash -lc '
  cd /NoSQLMark/backbench/logs
  tar -cf - timeseries*.log
' | tar -xf - -C "$RUN_DIR/nosqlmark/backend-logs"
wait "$CAPTURE_PID"
kc -n redis-operator-system logs deployment/redis-operator-controller-manager \
  -c manager --timestamps --since-time="$START_UTC" > "$RUN_DIR/controller.log"
k get rediscluster "$CLUSTER" -o yaml > "$RUN_DIR/cluster-after.yaml"
k exec "$CLUSTER-0" -c redis -- redis-cli cluster nodes > "$RUN_DIR/slots-after.txt"
master_counts > "$RUN_DIR/counts-after.txt"
pod_snapshot > "$RUN_DIR/pods-after.json"
curl -fsSG http://127.0.0.1:19090/api/v1/query_range \
  --data-urlencode "query=$CPU_CORES" --data-urlencode "start=$START_EPOCH" \
  --data-urlencode "end=$END_EPOCH" --data-urlencode 'step=5s' > "$RUN_DIR/cpu.json"
python3 "$EXP_HOME/utils/write_run_metadata.py"
```

Require the final master count to remain `RECORD_COUNT`, four masters to own
slots/data, no open slots, and the former standby to have gained keys. Confirm
`Reshard job succeeded` and `Successfully provisioned new standby` in the controller
log. New pods 8 and 9 are expected; compare restart counts only for pods present
before the run. Missing reshard markers, retries, client/Redis restarts or OOMs,
or no full minute of measured traffic after provisioning make the run inconclusive.

After preserving results, disable scaling `[CLUSTER CHANGE]`, including if the
run was inconclusive. If resharding/provisioning is still in progress, wait for that
operation to finish first; do not interrupt it midway by disabling reconciliation.
A stuck/failed operation needs diagnosis rather than an automatic cleanup command.

```bash
k get rediscluster "$CLUSTER" -o json | jq '{masters:.spec.masters,status:.status}'
k patch rediscluster "$CLUSTER" --type merge \
  --patch '{"spec":{"autoScaleEnabled":false}}'
k get rediscluster "$CLUSTER" -o jsonpath='{.spec.autoScaleEnabled}{"\n"}'
```

Stop Backend/REPL with `Ctrl-C` `[POD CHANGE]`, then stop the Prometheus port-forward.
Check the exported files before retiring this run with section 10. Disabling
scaling and stopping Backend/REPL do not release the Redis/client pod reservations.
Each repetition uses a fresh `RUN_ID` and namespace.

Keep the complete `$RUN_DIR` under `rebalancing-latency-experiment-artifacts/`
for later analysis, especially:

- `nosqlmark/backend-logs/timeseries*.log`: individual read/update latencies and timing.
- `nosqlmark/result/{summary,workload}.json` and `run.json`: counts, failures,
  workload settings, target rate, threshold, and run timestamps.
- `reshard.log` and `controller.log`: migration boundaries, donor/recipient,
  scaling reason, and standby provisioning completion.
- `cpu.json`, before/after pod and slot snapshots, key counts, and load/calibration
  records: utilization and checks that the run was valid.

Plotting is deferred. For later analysis, use Pre = the 60 seconds before scaling
is enabled; Reshard = `=== Resharding ... slots ===` through
`=== Re-enabling full coverage ===`; Post = 60 seconds after both migration and
standby provisioning finish. Compute p95 from individual operations. The discovery
rule remains: Reshard read p95 at least 25% above both Pre and Post, Post at most
15% above Pre, and at least three full one-second Reshard bins 25% above Pre.
Require the effect in at least two of three valid fresh runs. Raw logs and
summaries must agree on operation counts; timestamps must cover all three windows.

## 10. Retire the completed experiment namespace

Run this after section 9, once the exported results, raw latency logs,
metrics, and controller/reshard logs have been checked locally. Finish any
resharding/provisioning, disable this run's autoscaling, and stop Backend/REPL as
in section 9. Keep a failed or incomplete run available until its diagnosis and
necessary exports are finished; do not use cleanup to interrupt an operation.

This deletes only `foxtrot-rebalance-$RUN_ID` and its namespaced resources,
including the experiment RedisCluster, Redis/client pods, jobs, services,
ServiceMonitor, and PVCs. The existing StorageClass uses `Delete`: removing these
PVCs also triggers deletion of their PVs and backing EBS volumes, permanently
discarding this run's Redis data. The block verifies the actual PV reclaim policies
before deletion. See [Kubernetes volume reclamation](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#delete).
Local artifacts are retained; they are **not a backup of the Redis data**.

**The original `foxtrot` namespace, its RedisCluster, StatefulSet, and PVCs are
preserved.** The shared Redis operator, CRD, monitoring, storage infrastructure,
and other experiment namespaces are also retained. The only mutating command
below deletes the exact experiment namespace; it does not delete PVs directly or
use a wildcard. Cleanup releases this run's pod reservations before the next run,
including the client reservation on the fixed client node.

**Main** — `[CLUSTER CHANGE; LOCAL CHANGE]`: use the completed run's environment
from section 1, not a newly generated `RUN_ID`. In a new shell, source that run's
saved `run-env.sh`. The guards require the known AWS context and matching local
run metadata; `foxtrot` cannot pass the namespace guard. Execute this block only
when the experiment's database contents can be discarded.

```bash
cleanup_kc() {
  kubectl --kubeconfig /users/adas2125/.kube/amit.kubeconfig \
    --context amit@chirag-m5a-large-0803-215701.k8s.local "$@"
}
original_foxtrot_identity() {
  cleanup_kc get namespace foxtrot -o json |
    jq -r '[.kind, .metadata.name, .metadata.uid] | @tsv'
  cleanup_kc -n foxtrot get redisclusters.cache.example.com/redis-cluster \
    statefulset/redis-cluster -o json |
    jq -r '.items[] | [.kind, .metadata.name, .metadata.uid] | @tsv' | sort
  cleanup_kc -n foxtrot get pvc -o json |
    jq -r '.items[] | [.kind, .metadata.name, .metadata.uid] | @tsv' | sort
}
(
set -euo pipefail
[[ "${RUN_ID:-}" =~ ^[0-9]{8}-[0-9]{6}$ ]]
test "${NS:-}" = "foxtrot-rebalance-$RUN_ID"
test "${CLUSTER:-}" = "redis-$RUN_ID"
test "$KCFG" = /users/adas2125/.kube/amit.kubeconfig
test "$CTX" = amit@chirag-m5a-large-0803-215701.k8s.local
jq -e --arg run "$RUN_ID" --arg ns "$NS" --arg cluster "$CLUSTER" \
  '.RUN_ID == $run and .NS == $ns and .CLUSTER == $cluster' "$RUN_DIR/run.json"
cleanup_kc -n "$NS" get redisclusters.cache.example.com "$CLUSTER" -o json |
  jq -e '.spec.autoScaleEnabled == false and
    (.status.isResharding // false) == false and
    (.status.isProvisioningStandby // false) == false and
    (.status.isDraining // false) == false'
cleanup_kc -n "$NS" get jobs -o json |
  jq -e 'all(.items[]; (.status.active // 0) == 0)'
original_foxtrot_identity > "$RUN_DIR/cleanup-foxtrot-before.tsv"
cleanup_kc get pv -o json |
  jq --arg ns "$NS" '[.items[] | select(.spec.claimRef.namespace == $ns)]' \
  > "$RUN_DIR/cleanup-pvs.json"
jq -e 'all(.[]; .spec.persistentVolumeReclaimPolicy == "Delete")' \
  "$RUN_DIR/cleanup-pvs.json"
cleanup_kc -n "$NS" get pods,pvc -o wide
printf 'Deleting completed experiment namespace: %s\n' "$NS"
cleanup_kc delete namespace "$NS" --wait=true --timeout=300s
)
```

**Main** — `[READ ONLY; LOCAL CHANGE]`: verify deletion and compare the original
namespace, RedisCluster, StatefulSet, and PVC UIDs with their values before cleanup.
The identity comparison must produce no differences. The final pod listing lets
you confirm that the original Redis pods remain ready.

```bash
(
set -euo pipefail
[[ "${RUN_ID:-}" =~ ^[0-9]{8}-[0-9]{6}$ ]]
test "${NS:-}" = "foxtrot-rebalance-$RUN_ID"
remaining_namespace="$(cleanup_kc get namespace "$NS" --ignore-not-found -o name)"
test -z "$remaining_namespace"
original_foxtrot_identity > "$RUN_DIR/cleanup-foxtrot-after.tsv"
diff -u "$RUN_DIR/cleanup-foxtrot-before.tsv" "$RUN_DIR/cleanup-foxtrot-after.tsv"
cleanup_kc -n foxtrot get statefulset/redis-cluster
cleanup_kc -n foxtrot get pods -l cluster=redis-cluster -o wide
cleanup_kc get pv -o json |
  jq --arg ns "$NS" '[.items[] | select(.spec.claimRef.namespace == $ns)]' \
  > "$RUN_DIR/cleanup-pvs-remaining.json"
jq -e 'length == 0' "$RUN_DIR/cleanup-pvs-remaining.json"
)
```

Namespace and volume deletion can take time. If deletion times out or PVs remain,
wait and repeat the read-only verification; use `cleanup-pvs.json` for the original
PV names and EBS volume handles when diagnosing storage cleanup. Do not force
namespace deletion or remove finalizers. Retained PVs or a stuck operation need
separate diagnosis. Start the next run only after cleanup is complete and capacity
is checked again; reuse the existing operator and monitoring.
