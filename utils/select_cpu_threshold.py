"""Read RUN_DIR calibration metrics using TARGET_RPS, STANDBY, and CLIENT.

Write the selected threshold to RUN_DIR/cpu-threshold.txt. Uses local files only.
"""

import json, math, os
from pathlib import Path
root = Path(os.environ['RUN_DIR']) / 'calibration'
rate = int(os.environ['TARGET_RPS'])
standby = os.environ['STANDBY']
def values(name):
    data = json.loads((root / name).read_text())
    assert data['status'] == 'success' and data['data']['result'], name
    out = {x['metric']['pod']: float(x['value'][1]) for x in data['data']['result']}
    assert all(math.isfinite(v) for v in out.values()), name
    return out
idle = values('idle-cpu.json')
cpu = values(f'cpu-{rate}.json')
mem = values(f'memory-{rate}.json')
cores = values(f'cores-{rate}.json')
assert len(cpu) == len(idle) == len(mem) == 4 and set(cpu) == set(idle) == set(mem)
assert standby in cpu
idle_max = max(v for p, v in idle.items() if p != standby)
loaded = max(v for p, v in cpu.items() if p != standby)
assert 20 <= loaded <= 70 and loaded - idle_max >= 5, (idle_max, loaded)
assert max(mem.values()) < 70 and cores[os.environ['CLIENT']] < 1.5
summary = json.loads((root / str(rate) / 'summary.json').read_text())
assert abs(float(summary['Overall']['Throughput(ops/sec)']) / rate - 1) <= .02
threshold = math.floor((idle_max + loaded) / 2)
assert 1 < threshold < loaded and threshold > idle_max
(root.parent / 'cpu-threshold.txt').write_text(str(threshold) + '\n')
print(f'Chosen rate: {rate} RPS; CPU threshold: {threshold}%')
