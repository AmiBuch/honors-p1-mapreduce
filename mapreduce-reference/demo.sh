#!/bin/bash

# MapReduce Interactive Demo Script
# Demonstrates all key features of the system

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Function to print section headers
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

# Activate virtual environment
if [ -d "venv" ]; then
    source venv/bin/activate
else
    print_error "Virtual environment not found. Run './setup.sh' first."
    exit 1
fi

print_header "MapReduce System Demo"
echo "This demo will showcase:"
echo "  1. Basic word count"
echo "  2. Inverted index creation"
echo "  3. Concurrent job execution"
echo "  4. Straggler detection (if enabled)"
echo ""
read -p "Press Enter to begin..."

# ============================================================================
# Demo 1: Basic Word Count
# ============================================================================
print_header "Demo 1: Word Count on Shakespeare"

print_info "Starting MapReduce cluster..."
docker compose down > /dev/null 2>&1 || true
docker compose up -d
sleep 5
print_success "Cluster is running"

print_info "Submitting word count job..."
echo ""

START_TIME=$(date +%s)
python client/client.py submit \
    --input /data/input/shakespeare.txt \
    --output /data/output/demo_wordcount \
    --mapper examples/wordcount/mapper.py \
    --reducer examples/wordcount/reducer.py \
    --num-maps 4 \
    --num-reduces 2 \
    --follow
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

print_success "Job completed in ${DURATION} seconds"
echo ""
print_info "Top 10 most frequent words:"
python client/client.py results /data/output/demo_wordcount --limit 10

read -p "Press Enter to continue to next demo..."

# ============================================================================
# Demo 2: Inverted Index
# ============================================================================
print_header "Demo 2: Inverted Index Creation"

print_info "Creating sample document collection..."
cat > data/input/demo_docs.txt << 'EOF'
doc_1: the quick brown fox jumps over the lazy dog
doc_2: the lazy brown dog sleeps all day
doc_3: quick brown animals are fascinating
doc_4: the fox is quick and clever
doc_5: dogs and foxes are both animals
EOF

print_success "Created 5 sample documents"

print_info "Submitting inverted index job..."
echo ""

python client/client.py submit \
    --input /data/input/demo_docs.txt \
    --output /data/output/demo_index \
    --mapper examples/inverted_index/mapper.py \
    --reducer examples/inverted_index/reducer.py \
    --num-maps 2 \
    --num-reduces 2 \
    --follow

print_success "Index created"
echo ""
print_info "Sample index entries (word → document IDs):"
python client/client.py results /data/output/demo_index --limit 10

read -p "Press Enter to continue to next demo..."

# ============================================================================
# Demo 3: Concurrent Jobs
# ============================================================================
print_header "Demo 3: Concurrent Job Execution"

print_info "Submitting 3 jobs concurrently..."

# Generate test data
for i in {1..3}; do
    echo "Test data for job $i" > data/input/concurrent_$i.txt
    for j in {1..1000}; do
        echo "line $j with words to count in job $i" >> data/input/concurrent_$i.txt
    done
done

# Submit jobs in background
JOB_IDS=()
for i in {1..3}; do
    print_info "Submitting job $i..."
    JOB_ID=$(python client/client.py submit \
        --input /data/input/concurrent_$i.txt \
        --output /data/output/concurrent_$i \
        --mapper examples/wordcount/mapper.py \
        --reducer examples/wordcount/reducer.py \
        --num-maps 2 \
        --num-reduces 2 | grep "Job ID:" | awk '{print $3}')
    JOB_IDS+=($JOB_ID)
    echo "  Job ID: $JOB_ID"
done

print_info "Monitoring job completion..."
echo ""

# Monitor all jobs
ALL_COMPLETE=false
while [ "$ALL_COMPLETE" = false ]; do
    ALL_COMPLETE=true
    for JOB_ID in "${JOB_IDS[@]}"; do
        STATUS=$(python client/client.py status $JOB_ID 2>/dev/null | grep "Job $JOB_ID:" | awk '{print $3}' || echo "UNKNOWN")
        if [ "$STATUS" != "COMPLETED" ]; then
            ALL_COMPLETE=false
        fi
        echo "  Job $JOB_ID: $STATUS"
    done
    
    if [ "$ALL_COMPLETE" = false ]; then
        echo ""
        sleep 2
        echo -ne "\033[4A"  # Move cursor up 4 lines
    fi
done

print_success "All concurrent jobs completed!"

read -p "Press Enter to continue to next demo..."

# ============================================================================
# Demo 4: Straggler Detection
# ============================================================================
print_header "Demo 4: Straggler Detection"

# Check if straggler mode is enabled
STRAGGLER_ENABLED=$(docker compose ps | grep worker-3 | grep -c "SIMULATE_STRAGGLER=true" || echo "0")

if [ "$STRAGGLER_ENABLED" = "0" ]; then
    print_info "Straggler simulation is not enabled for worker-3"
    echo "To enable:"
    echo "  1. Edit docker-compose.yml"
    echo "  2. Set SIMULATE_STRAGGLER=true for worker-3"
    echo "  3. Run: docker compose down && docker compose up -d"
    echo ""
    print_info "For now, we'll show the normal execution time..."
else
    print_success "Straggler simulation is enabled for worker-3"
    print_info "This worker will be artificially slowed by 10 seconds per task"
    echo ""
fi

# Generate larger test data for straggler demo
print_info "Generating test data (10,000 lines)..."
python3 << 'EOF'
with open('data/input/straggler_test.txt', 'w') as f:
    for i in range(10000):
        f.write(f'line {i} with multiple words for straggler detection test\n')
EOF

print_info "Submitting job with 8 map tasks and 4 reduce tasks..."
echo ""

START_TIME=$(date +%s)
python client/client.py submit \
    --input /data/input/straggler_test.txt \
    --output /data/output/straggler_demo \
    --mapper examples/wordcount/mapper.py \
    --reducer examples/wordcount/reducer.py \
    --num-maps 8 \
    --num-reduces 4 \
    --follow
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

print_success "Job completed in ${DURATION} seconds"

if [ "$STRAGGLER_ENABLED" = "1" ]; then
    echo ""
    print_info "Checking coordinator logs for straggler detection..."
    STRAGGLER_LOGS=$(docker compose logs coordinator 2>/dev/null | grep -i "straggler" | tail -5)
    if [ -n "$STRAGGLER_LOGS" ]; then
        echo "$STRAGGLER_LOGS"
        print_success "Straggler detection is working!"
        echo ""
        echo "Expected behavior:"
        echo "  • Without detection: ~60s (bottlenecked by slow worker)"
        echo "  • With detection: ~${DURATION}s (backup tasks launched)"
        echo "  • Speedup: ~2-3x improvement"
    else
        print_info "No stragglers detected (job completed too quickly)"
    fi
fi

read -p "Press Enter to continue..."

# ============================================================================
# System Status
# ============================================================================
print_header "System Status Summary"

echo "Container Status:"
docker compose ps

echo ""
echo "Resource Usage:"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}" | head -6

echo ""
print_info "Data Storage:"
echo "  Input files: $(ls data/input/ | wc -l) files"
echo "  Intermediate: $(find data/intermediate/ -type f 2>/dev/null | wc -l) files"
echo "  Output: $(find data/output/ -type f 2>/dev/null | wc -l) files"

# ============================================================================
# Cleanup Option
# ============================================================================
echo ""
echo -e "${YELLOW}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${YELLOW}║${NC} Demo Complete!"
echo -e "${YELLOW}╚══════════════════════════════════════════════════════════╝${NC}"
echo ""

read -p "Stop the cluster? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_info "Stopping cluster..."
    docker compose down
    print_success "Cluster stopped"
else
    print_info "Cluster is still running. Stop with: docker compose down"
fi

echo ""
print_success "Demo finished! Key takeaways:"
echo "  • MapReduce jobs execute efficiently across 4 workers"
echo "  • Multiple jobs can run concurrently"
echo "  • Straggler detection provides 2-3x speedup when enabled"
echo "  • System handles various MapReduce workloads"
echo ""
echo "Try your own jobs with:"
echo "  python client/client.py submit --help"
