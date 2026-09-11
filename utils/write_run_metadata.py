"""Save exported run identity, settings, and timestamps to RUN_DIR/run.json."""

import json, os
from pathlib import Path
fields = ['RUN_ID', 'NS', 'CLUSTER', 'TARGET_RPS', 'CPU_THRESHOLD',
          'START_EPOCH', 'ENABLE_EPOCH', 'END_EPOCH']
(Path(os.environ['RUN_DIR']) / 'run.json').write_text(
    json.dumps({k: os.environ[k] for k in fields}, indent=2) + '\n')
