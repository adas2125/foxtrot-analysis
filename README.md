# Experiment Commands
The FoxTrot repository is available here ([FoxTrot](https://github.com/SatyamS17/redis-foxtrot-autoscaler/tree/main)). Below is a brief summary of the commands used for a preliminary experiment w/ FoxTrot. These commands are not exhaustive and are slightly simplified to highlight the main result-generation workflow.

### Forward Prometheus Port
```sh
kubectl port-forward \
  -n monitoring \
  service/kps-kube-prometheus-stack-prometheus \
  19090:9090 \
  > port-forward.log 2>&1 &

curl -fsS http://127.0.0.1:19090/-/ready
```

### Create Timestamped Results Directory
```sh
HOTSPOT_RUN_DIR="$HOME/stateful-scaling/results/hotspot-single-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$HOTSPOT_RUN_DIR"
```

### Create Hotspot Runner Pod
```sh
kubectl run hotspot-runner \
  -n foxtrot \
  --image=python:3.11-slim \
  --restart=Never \
  --command -- sleep infinity

kubectl wait \
  -n foxtrot \
  --for=condition=Ready \
  pod/hotspot-runner \
  --timeout=180s
  ```

### Install Redis Client
```sh
kubectl exec -n foxtrot hotspot-runner -- \
  python -m pip install --no-cache-dir 'redis==5.0.8'
```

### Copy Hotspot Script to Pod
```sh
kubectl cp \
  "$HOTSPOT_RUN_DIR/hotspot.py" \
  foxtrot/hotspot-runner:/hotspot.py
```

### Configure Autoscaling Parameters
```sh
kubectl patch rediscluster redis-cluster \
  -n foxtrot \
  --type merge \
  --patch '{
    "spec": {
      "cpuThreshold": 10,
      "cpuThresholdLow": 5,
      "scaleCooldownSeconds": 1800,
      "autoScaleEnabled": false
    }
  }'
```

### Record Workload Start Time
```sh
WORKLOAD_START_EPOCH=$(date +%s)
WORKLOAD_START_UTC=$(date -u +%Y-%m-%dT%H:%M:%SZ)
TS_START=$((WORKLOAD_START_EPOCH - 60))
TS_END=$((WORKLOAD_START_EPOCH + 600))
```

### Start Closed-Loop Hotspot Workload
```sh
(
  kubectl exec -n foxtrot hotspot-runner -- \
    env HOTSPOT_THREADS=10 \
    python -u /hotspot.py redis-cluster-headless 100 600 \
    > "$HOTSPOT_RUN_DIR/hotspot-main.log" 2>&1
) &
```

# Enable autoscaling
```sh
kubectl patch rediscluster redis-cluster \
  -n foxtrot \
  --type merge \
  --patch '{"spec":{"autoScaleEnabled":true}}'
```

### Collect Redis Master CPU Usage & Plot Results
```sh
curl -sG 'http://127.0.0.1:19090/api/v1/query_range' \
  --data-urlencode 'query=(sum by (pod) (rate(container_cpu_usage_seconds_total{container="redis",namespace="foxtrot",pod=~"^redis-cluster-.*",service="kps-kube-prometheus-stack-kubelet"}[1m])) * 100) and on(pod) redis_instance_info{role="master"}' \
  --data-urlencode "start=$TS_START" \
  --data-urlencode "end=$TS_END" \
  --data-urlencode 'step=5' \
  > "$HOTSPOT_RUN_DIR/redis-cpu-timeseries.json"

python3 "$HOTSPOT_RUN_DIR/plot_hotspot.py" "$HOTSPOT_RUN_DIR"
```
