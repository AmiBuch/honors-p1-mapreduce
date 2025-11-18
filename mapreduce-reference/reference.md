# MapReduce Quick Reference Card

## Essential Commands

### Setup & Start
```bash
./setup.sh                                    # Initial setup
docker compose up -d                          # Start cluster
docker compose ps                             # Check status
source venv/bin/activate                      # Activate Python env
```

### Run Demonstrations
```bash
./quick_demo.sh                               # 5-minute validation
./scripts/demo_straggler_detection.sh         # Straggler demo (15 min)
./scripts/run_performance_tests.sh            # Full evaluation (20 min)
./scripts/complete_demo.sh                    # All demos (30 min)
```

### Submit Jobs
```bash
# Basic job
python client/client.py submit \
  --input /data/input/<file> \
  --output /data/output/<dir> \
  --mapper examples/<type>/mapper.py \
  --reducer examples/<type>/reducer.py \
  --num-maps 8 \
  --num-reduces 4 \
  --follow

# View results
python client/client.py results /data/output/<dir>
```

### Straggler Control
```bash
./scripts/toggle_straggler.sh enable          # Enable simulation
./scripts/toggle_straggler.sh disable         # Disable simulation
./scripts/toggle_straggler.sh status          # Check status
```

### Monitoring
```bash
docker compose logs -f coordinator            # Coordinator logs
docker compose logs -f worker-3               # Worker logs
docker compose logs | grep -i straggler       # Find straggler events
docker stats --no-stream                      # Resource usage
```

### Testing
```bash
pytest tests/ -v                              # Run all tests
pytest tests/test_mapreduce.py::TestStraggler -v  # Straggler tests only
```

## Performance Results

### Expected Times (100K lines, 16 map tasks, 8 reduce tasks)

| Scenario | Time | Description |
|----------|------|-------------|
| **Baseline** | 20s | All workers normal |
| **No Detection** | 60s | Straggler bottlenecks job |
| **With Detection** | 25s | Backup tasks mitigate |

### Key Metric
**2.4x speedup** with straggler detection (25s vs 60s)

## File Locations

### Core Files
- `coordinator/server.py` - Job scheduler, straggler detection
- `worker/worker.py` - Task execution
- `client/client.py` - CLI interface

### Examples
- `examples/wordcount/` - Word counting
- `examples/url_analyzer/` - Web log analysis
- `examples/ngram/` - N-gram extraction
- `examples/inverted_index/` - Search indexing

### Scripts
- `scripts/demo_straggler_detection.sh` - Interactive straggler demo
- `scripts/run_performance_tests.sh` - 3-way performance comparison
- `scripts/toggle_straggler.sh` - Enable/disable straggler
- `scripts/generate_test_data.py` - Create test files

### Documentation
- `README.md` - Quick start guide
- `COMPREHENSIVE_DOCUMENTATION.md` - Complete reference
- `docs/DEMO_GUIDE.md` - Presentation script

## Troubleshooting

### Containers not running?
```bash
docker compose down
docker compose up -d
sleep 10
docker compose ps
```

### Jobs not completing?
```bash
docker compose logs coordinator | tail -50
docker compose logs worker-1 | tail -30
```

### No straggler detection?
```bash
./scripts/toggle_straggler.sh status
docker compose logs coordinator | grep -i straggler
# Use --num-maps 16 or higher
```

### Reset everything
```bash
docker compose down -v
rm -rf data/intermediate/* data/output/*
docker compose build --no-cache
docker compose up -d
```

## Quick Examples

### Word Count
```bash
python client/client.py submit \
  --input /data/input/shakespeare.txt \
  --output /data/output/wc \
  --mapper examples/wordcount/mapper.py \
  --reducer examples/wordcount/reducer.py \
  --num-maps 4 --num-reduces 2 --follow
```

### URL Analysis
```bash
python client/client.py upload examples/data/server_logs.txt /data/input/logs.txt
python client/client.py submit \
  --input /data/input/logs.txt \
  --output /data/output/urls \
  --mapper examples/url_analyzer/mapper.py \
  --reducer examples/url_analyzer/reducer.py \
  --num-maps 2 --num-reduces 2 --follow
```

## Straggler Detection Demo

```bash
# Show the problem (60s)
./scripts/toggle_straggler.sh enable
# Modify coordinator to disable detection (threshold=999.0)
# Run job → Takes 60s

# Show the solution (25s)
# Restore coordinator (threshold=1.5)
# Run same job → Takes 25s
# See detection events in logs
```

## For Documentation/Screenshots

1. **System status:** `docker compose ps && docker stats --no-stream`
2. **Job running:** Submit with `--follow`, capture output
3. **Results:** `python client/client.py results /data/output/<job>`
4. **Straggler logs:** `docker compose logs coordinator | grep straggler`
5. **Performance:** `cat performance_results.txt`

## Success Criteria

✓ 5 containers running  
✓ Jobs complete successfully  
✓ Straggler detection events visible  
✓ 2.4x speedup in performance tests  
✓ All pytest tests pass  

---

**Need More Detail?** See `COMPREHENSIVE_DOCUMENTATION.md`
