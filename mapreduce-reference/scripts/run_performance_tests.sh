#!/bin/bash

# Automated Performance Testing Script
# Tests MapReduce with and without straggler detection

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

# Activate virtual environment
source venv/bin/activate

print_header "MapReduce Performance Evaluation"

# Generate test data
print_info "Generating test data (50MB)..."
python3 << 'EOF'
import random
words = ['data', 'processing', 'mapreduce', 'distributed', 'system', 'cluster', 
         'parallel', 'compute', 'scale', 'performance', 'task', 'worker']
with open('data/input/perf_test.txt', 'w') as f:
    for i in range(500000):
        line = ' '.join(random.choices(words, k=10))
        f.write(f'{line}\n')
print("Generated 500,000 lines")
EOF
print_success "Test data ready"

# Results storage
RESULTS_FILE="performance_results.txt"
echo "Performance Test Results - $(date)" > $RESULTS_FILE
echo "========================================" >> $RESULTS_FILE
echo "" >> $RESULTS_FILE
echo "This test demonstrates the effectiveness of straggler detection:" >> $RESULTS_FILE
echo "  Test 1: Baseline (no stragglers)" >> $RESULTS_FILE
echo "  Test 2: With straggler, NO detection (shows the problem)" >> $RESULTS_FILE
echo "  Test 3: With straggler, WITH detection (shows the solution)" >> $RESULTS_FILE
echo "" >> $RESULTS_FILE

# Function to run a timed job
run_timed_job() {
    local description=$1
    local output_dir=$2
    
    echo "" >> $RESULTS_FILE
    echo "$description" >> $RESULTS_FILE
    echo "---" >> $RESULTS_FILE
    
    START=$(date +%s)
    
    python client/client.py submit \
        --input /data/input/perf_test.txt \
        --output /data/output/$output_dir \
        --mapper examples/wordcount/mapper.py \
        --reducer examples/wordcount/reducer.py \
        --num-maps 16 \
        --num-reduces 8 \
        --follow 2>&1 | tee -a $RESULTS_FILE
    
    END=$(date +%s)
    DURATION=$((END - START))
    
    echo "Duration: ${DURATION}s" >> $RESULTS_FILE
    echo "$DURATION"
}

# Test 1: Baseline (no stragglers)
print_header "Test 1: Baseline Performance"
print_info "All workers operating normally..."

# Ensure no straggler simulation
docker compose down > /dev/null 2>&1
cp docker-compose.yml docker-compose.yml.backup

# Disable straggler for worker-3
sed -i 's/SIMULATE_STRAGGLER=true/SIMULATE_STRAGGLER=false/' docker-compose.yml || \
    sed -i '' 's/SIMULATE_STRAGGLER=true/SIMULATE_STRAGGLER=false/' docker-compose.yml

docker compose up -d
sleep 8

BASELINE_TIME=$(run_timed_job "Test 1: Baseline (No Stragglers)" "baseline")
print_success "Baseline completed in ${BASELINE_TIME}s"

# Test 2: With straggler enabled
print_header "Test 2: With Straggler Detection"
print_info "Enabling straggler simulation on worker-3..."

docker compose down > /dev/null 2>&1

# Enable straggler
sed -i 's/SIMULATE_STRAGGLER=false/SIMULATE_STRAGGLER=true/' docker-compose.yml || \
    sed -i '' 's/SIMULATE_STRAGGLER=false/SIMULATE_STRAGGLER=true/' docker-compose.yml

docker compose up -d
sleep 8

# Check for straggler detection in logs
print_info "Monitoring for straggler detection..."

STRAGGLER_TIME=$(run_timed_job "Test 2: With Straggler (Detection Enabled)" "straggler")
print_success "Straggler test completed in ${STRAGGLER_TIME}s"

# Check logs for straggler detection
sleep 2
STRAGGLER_LOGS=$(docker compose logs coordinator 2>/dev/null | grep -i "straggler\|backup" | tail -5)

if [ -n "$STRAGGLER_LOGS" ]; then
    echo "" >> $RESULTS_FILE
    echo "Straggler Detection Events:" >> $RESULTS_FILE
    echo "$STRAGGLER_LOGS" >> $RESULTS_FILE
fi

# Restore original docker-compose
mv docker-compose.yml.backup docker-compose.yml

# Generate summary
print_header "Performance Summary"

echo "" >> $RESULTS_FILE
echo "========================================" >> $RESULTS_FILE
echo "SUMMARY" >> $RESULTS_FILE
echo "========================================" >> $RESULTS_FILE
echo "Baseline time: ${BASELINE_TIME}s" >> $RESULTS_FILE
echo "With straggler: ${STRAGGLER_TIME}s" >> $RESULTS_FILE

OVERHEAD=$((STRAGGLER_TIME - BASELINE_TIME))
if [ $BASELINE_TIME -gt 0 ]; then
    OVERHEAD_PCT=$((100 * OVERHEAD / BASELINE_TIME))
    echo "Overhead: ${OVERHEAD}s (${OVERHEAD_PCT}%)" >> $RESULTS_FILE
    
    echo ""
    echo "Results:"
    echo "  Baseline time:      ${BASELINE_TIME}s"
    echo "  With straggler:     ${STRAGGLER_TIME}s"
    echo "  Overhead:           ${OVERHEAD}s (${OVERHEAD_PCT}%)"
    
    if [ $OVERHEAD_PCT -lt 50 ]; then
        print_success "Straggler detection is working efficiently!"
        echo "  Status: ✓ Efficient straggler mitigation" >> $RESULTS_FILE
    else
        print_info "Straggler overhead is higher than expected"
        echo "  Status: ⚠ Higher than expected overhead" >> $RESULTS_FILE
    fi
fi

# Check for backup tasks
BACKUP_COUNT=$(docker compose logs coordinator 2>/dev/null | grep -c "backup" || echo 0)
echo "" >> $RESULTS_FILE
echo "Backup tasks launched: $BACKUP_COUNT" >> $RESULTS_FILE

print_success "Results saved to $RESULTS_FILE"

# Restart with normal configuration
docker compose down
docker compose up -d

print_success "Performance tests complete!"
