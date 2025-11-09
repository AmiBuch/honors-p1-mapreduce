# Design Document — Honors Project 1: MapReduce

**Student:** Ami Buch  
**Section:** Section 1    
---

## Client Interface

### Commands
```bash
# Upload data to shared storage
python client.py upload input.txt /data/input/input.txt

# Submit a MapReduce job
python client.py submit \
  --input /data/input/input.txt \
  --output /data/output/result \
  --mapper mapper.py \
  --reducer reducer.py \
  --num-maps 8 \
  --num-reduces 4

# Check job status
python client.py status <job_id>
```

---

## Implementation Language

**Python 3.10+**

Fast to prototype, good libraries for distributed systems, easy to dynamically load user map/reduce functions.

---

## Communication Protocol

**gRPC with Protocol Buffers**

- Binary protocol is faster than JSON/HTTP
- Built-in timeouts and deadlines (critical for straggler detection)
- Type safety via protobuf schemas
- Industry standard (Kubernetes, Hadoop 3.x)

**Key RPCs:**
- Client → Coordinator: SubmitJob, GetJobStatus
- Coordinator → Workers: ExecuteMapTask, ExecuteReduceTask, Heartbeat

---

## Shared Storage

**Docker volume mount** across all containers (mounted at `/data/`)

Simple for single-VM deployment, direct filesystem access, easy to debug.

**Directory structure:**
```
/data/
├── input/          # Raw input files
├── intermediate/   # Map outputs (partitioned by reduce task)
└── output/         # Final results
```

---

## Data Formats

| Type | Format | Rationale |
|------|--------|-----------|
| Input | Plain text | Universal, human-readable |
| Intermediate | Protocol Buffers | Compact, type-safe, pairs with gRPC |
| Output | Plain text | Human-readable results |

---

## Testing Plan

**Functional Tests:**
- Word count correctness
- Empty input handling
- Multiple concurrent jobs

**Straggler Detection Tests:**
- Single slow worker (inject 10s delay)
- Multiple slow workers
- No false positives when all workers normal
- Verify backup tasks launched and completed

**Integration Test:**
- End-to-end job on 1GB text file

---

## Performance Evaluation

### Metrics
- Job completion time (with vs without straggler detection)
- Number of backup tasks launched
- Task duration distribution

### Experimental Setup
- Inject artificial delay (10s) into worker-3
- Run same job three ways:
  1. Baseline (no stragglers)
  2. With straggler, detection disabled
  3. With straggler, detection enabled

### Expected Results
- Baseline: ~20s
- Straggler without detection: ~60s
- Straggler with detection: ~25s
- **Speedup: 2.4x**

### Plots
1. **Bar chart:** Job time comparison across three scenarios
2. **Timeline:** Gantt chart showing when backup tasks launched
3. **CDF:** Task duration distribution showing tail is clipped

---

## Special Feature: Straggler Detection

### Problem
One slow worker can bottleneck the entire job. Common causes: hardware issues, resource contention, network delays.

### Solution
Monitor task durations. If a task takes >1.5x the median time, launch a backup copy on a different worker. Accept whichever finishes first.

### Algorithm
```python
# Wait until 25% of tasks complete (establish baseline)
# Calculate median task duration
# For each active task:
#   if elapsed > 1.5 * median and no backup exists:
#     launch backup on different worker
# Accept first completion, cancel duplicate
```
---

## Example: Word Count

**Mapper:**
```python
def mapper(line):
    for word in line.strip().split():
        yield (word.lower(), 1)
```

**Reducer:**
```python
def reducer(key, values):
    yield (key, sum(values))
```

**With Straggler:**
Worker-3 has artificial 10s delay per task. System detects slowdown, launches backup on Worker-1, job completes faster.
