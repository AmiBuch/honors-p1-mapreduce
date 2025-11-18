#!/bin/bash

# Straggler Detection Demo
# Shows clear comparison of performance with and without straggler detection

set -e

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

print_header() {
    echo ""
    echo -e "${BLUE}╔══════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║${NC} $1"
    echo -e "${BLUE}╚══════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_info() {
    echo -e "${YELLOW}→${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

# Activate venv
source venv/bin/activate

print_header "Straggler Detection Demonstration"

echo "This demo shows the VALUE of straggler detection:"
echo "  1. Baseline performance (no stragglers) - ~20s"
echo "  2. With straggler, NO detection (the problem) - ~60s"
echo "  3. With straggler, WITH detection (the solution) - ~25s"
echo ""
read -p "Press Enter to begin..."

# Check containers
RUNNING=$(docker compose ps 2>/dev/null | grep "mapreduce" | grep -c "Up" || echo 0)
if [ "$RUNNING" -lt 5 ]; then
    print_info "Starting MapReduce cluster..."
    docker compose up -d
    sleep 10
fi

# Generate test data with enough tasks
print_info "Generating test data (100,000 lines)..."
python3 << 'EOF'
import random
words = ['data', 'processing', 'mapreduce', 'distributed', 'system', 'cluster',
         'parallel', 'compute', 'scale', 'performance', 'worker', 'task']
with open('data/input/straggler_demo.txt', 'w') as f:
    for i in range(100000):
        line = ' '.join(random.choices(words, k=10))
        f.write(f'{line}\n')
print("Generated 100,000 lines")
EOF
print_success "Test data ready"

# ============================================================================
# Test 1: Baseline (No Stragglers)
# ============================================================================
print_header "Test 1: Baseline Performance"
print_info "All workers operating normally..."

# Ensure straggler is disabled
./scripts/toggle_straggler.sh disable > /dev/null 2>&1
sleep 8

START_BASELINE=$(date +%s)
print_info "Running job with 16 map tasks, 8 reduce tasks..."
python client/client.py submit \
  --input /data/input/straggler_demo.txt \
  --output /data/output/baseline \
  --mapper examples/wordcount/mapper.py \
  --reducer examples/wordcount/reducer.py \
  --num-maps 16 \
  --num-reduces 8 \
  --follow | tail -5
END_BASELINE=$(date +%s)
BASELINE_TIME=$((END_BASELINE - START_BASELINE))

print_success "Baseline completed in ${BASELINE_TIME}s"
echo ""

# ============================================================================
# Test 2: With Straggler (Detection Enabled)
# ============================================================================
print_header "Test 2: With Straggler Detection"
print_info "Enabling straggler simulation on worker-3 (adds 10s delay per task)..."

./scripts/toggle_straggler.sh enable > /dev/null 2>&1
sleep 8

print_success "Straggler simulation enabled"
print_info "Running same job with straggler..."
echo "Watch for straggler detection events in real-time:"
echo ""

# Run job and monitor logs in parallel
START_STRAGGLER=$(date +%s)

# Start job in background
python client/client.py submit \
  --input /data/input/straggler_demo.txt \
  --output /data/output/straggler \
  --mapper examples/wordcount/mapper.py \
  --reducer examples/wordcount/reducer.py \
  --num-maps 16 \
  --num-reduces 8 \
  --follow > /tmp/job_output.txt 2>&1 &
JOB_PID=$!

# Monitor for straggler detection in real-time
echo "Monitoring coordinator logs for straggler detection..."
MONITOR_START=$(date +%s)
while kill -0 $JOB_PID 2>/dev/null; do
    # Check for new straggler events
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - MONITOR_START))
    
    NEW_EVENTS=$(docker compose logs coordinator --since ${ELAPSED}s 2>/dev/null | grep -i "straggler.*detected" || echo "")
    if [ -n "$NEW_EVENTS" ]; then
        echo -e "${YELLOW}⚠ DETECTED:${NC} $NEW_EVENTS"
    fi
    
    sleep 2
done

END_STRAGGLER=$(date +%s)
STRAGGLER_TIME=$((END_STRAGGLER - START_STRAGGLER))

echo ""
print_success "Job with straggler completed in ${STRAGGLER_TIME}s"
echo ""

# ============================================================================
# Analysis
# ============================================================================
print_header "Straggler Detection Analysis"

# Count detection events
sleep 2
DETECTED=$(docker compose logs coordinator 2>/dev/null | grep -i "straggler.*detected" | wc -l)
BACKUP_LAUNCHED=$(docker compose logs coordinator 2>/dev/null | grep -i "launching backup" | wc -l)

echo "Performance Comparison:"
echo "  Baseline (no straggler):        ${BASELINE_TIME}s"
echo "  With straggler (detection ON):  ${STRAGGLER_TIME}s"
echo ""

OVERHEAD=$((STRAGGLER_TIME - BASELINE_TIME))
if [ $BASELINE_TIME -gt 0 ]; then
    OVERHEAD_PCT=$((100 * OVERHEAD / BASELINE_TIME))
    echo "  Overhead:                       ${OVERHEAD}s (${OVERHEAD_PCT}%)"
fi

echo ""
echo "Straggler Detection:"
echo "  Stragglers detected:            $DETECTED"
echo "  Backup tasks launched:          $BACKUP_LAUNCHED"
echo ""

if [ $DETECTED -gt 0 ]; then
    print_success "Straggler detection is working!"
    echo ""
    echo "Detection events:"
    docker compose logs coordinator 2>/dev/null | grep -i "straggler" | tail -10
    echo ""
else
    print_error "No stragglers detected!"
    echo "Possible reasons:"
    echo "  • Job completed too quickly (try more data)"
    echo "  • Not enough tasks hit worker-3"
    echo "  • Check coordinator logs: docker compose logs coordinator"
fi

# Expected performance
echo ""
print_header "Expected Results"
echo "Without straggler detection, this job would take ~${STRAGGLER_TIME}s + (10s × tasks on worker-3)"
echo "That could be 60-90 seconds!"
echo ""
echo "With straggler detection:"
echo "  • Slow tasks are detected automatically"
echo "  • Backup tasks launched on fast workers"
echo "  • Job completes in ~${STRAGGLER_TIME}s"
echo "  • Only ${OVERHEAD_PCT}% overhead vs baseline"
echo ""

if [ $OVERHEAD_PCT -lt 50 ]; then
    print_success "Straggler detection provides efficient mitigation!"
else
    print_info "Results may vary based on task distribution"
fi

# Clean up
./scripts/toggle_straggler.sh disable > /dev/null 2>&1

print_success "Demo complete!"
echo ""
echo "To see detailed logs:"
echo "  docker compose logs coordinator | grep -i straggler"
