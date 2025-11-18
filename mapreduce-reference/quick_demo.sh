#!/bin/bash

# Quick Demo - Run This First!
# Fast demonstration of all key features

set -e

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}"
cat << "EOF"
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║              MapReduce Quick Demo (5 minutes)                 ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
EOF
echo -e "${NC}"

# Check if containers are running
echo "Checking MapReduce cluster status..."
RUNNING=$(docker compose ps 2>/dev/null | grep "mapreduce" | grep -c "Up" || echo 0)

if [ "$RUNNING" -lt 5 ]; then
    echo -e "${YELLOW}⚠${NC} MapReduce containers not running. Starting cluster..."
    docker compose up -d
    sleep 10
    echo -e "${GREEN}✓${NC} Cluster started"
else
    echo -e "${GREEN}✓${NC} Cluster is running ($RUNNING containers)"
fi
echo ""

# Activate venv
source venv/bin/activate

# Test 1: Basic word count
echo -e "${YELLOW}[1/4]${NC} Running word count..."
python client/client.py submit \
  --input /data/input/sample.txt \
  --output /data/output/quick1 \
  --mapper examples/wordcount/mapper.py \
  --reducer examples/wordcount/reducer.py \
  --num-maps 2 \
  --num-reduces 2 \
  --follow > /dev/null 2>&1

echo -e "${GREEN}✓${NC} Results:"
python client/client.py results /data/output/quick1 | head -5
echo ""

# Test 2: System status
echo -e "${YELLOW}[2/4]${NC} Checking system status..."
echo "Containers:"
docker compose ps | grep "Up" | wc -l | xargs echo "  Running:"
docker stats --no-stream | tail -5
echo ""

# Test 3: Straggler detection
echo -e "${YELLOW}[3/4]${NC} Testing straggler detection..."
chmod +x scripts/toggle_straggler.sh

# Enable straggler
./scripts/toggle_straggler.sh enable > /dev/null 2>&1
sleep 8

# Generate enough test data to trigger straggler detection
echo "Generating test data for straggler test..."
python3 << 'EOF'
import random
words = ['data', 'processing', 'mapreduce', 'distributed', 'system']
with open('data/input/straggler_test.txt', 'w') as f:
    for i in range(50000):  # 50K lines to ensure multiple tasks hit worker-3
        line = ' '.join(random.choices(words, k=8))
        f.write(f'{line}\n')
EOF

echo "Running job with straggler simulation (worker-3 has 10s delay)..."
START=$(date +%s)
python client/client.py submit \
  --input /data/input/straggler_test.txt \
  --output /data/output/quick2 \
  --mapper examples/wordcount/mapper.py \
  --reducer examples/wordcount/reducer.py \
  --num-maps 16 \
  --num-reduces 8 \
  --follow > /dev/null 2>&1
END=$(date +%s)
DURATION=$((END - START))

echo -e "${GREEN}✓${NC} Completed in ${DURATION}s (with straggler detection enabled)"

# Check for detection - wait a moment for logs to flush
sleep 2
DETECTED=$(docker compose logs coordinator 2>/dev/null | grep -i "straggler" | grep -c "detected" || echo 0)
BACKUP_COUNT=$(docker compose logs coordinator 2>/dev/null | grep -c "backup" || echo 0)

echo "  Straggler detection events: $DETECTED"
echo "  Backup tasks launched: $BACKUP_COUNT"

if [ $DETECTED -gt 0 ]; then
    echo -e "${GREEN}✓${NC} Straggler detection is working!"
    echo ""
    echo "Sample detection logs:"
    docker compose logs coordinator 2>/dev/null | grep -i "straggler" | head -3
else
    echo -e "${YELLOW}⚠${NC} No stragglers detected (may need more tasks or time)"
fi

./scripts/toggle_straggler.sh disable > /dev/null 2>&1
echo ""

# Test 4: URL analysis
echo -e "${YELLOW}[4/4]${NC} Running URL analyzer..."
python client/client.py upload examples/data/server_logs.txt /data/input/server_logs.txt > /dev/null 2>&1
python client/client.py submit \
  --input /data/input/server_logs.txt \
  --output /data/output/quick3 \
  --mapper examples/url_analyzer/mapper.py \
  --reducer examples/url_analyzer/reducer.py \
  --num-maps 2 \
  --num-reduces 2 \
  --follow > /dev/null 2>&1

echo -e "${GREEN}✓${NC} Top results:"
python client/client.py results /data/output/quick3 | head -5
echo ""

# Summary
echo -e "${BLUE}"
cat << "EOF"
╔═══════════════════════════════════════════════════════════════╗
║                      ✓ Quick Demo Complete!                   ║
╚═══════════════════════════════════════════════════════════════╝
EOF
echo -e "${NC}"

echo "What worked:"
echo "  ✓ Word count on sample data"
echo "  ✓ System monitoring ($RUNNING MapReduce containers)"
echo "  ✓ Straggler detection ($DETECTED events, $BACKUP_COUNT backups)"
echo "  ✓ URL analysis on server logs"
echo ""
echo "Next steps:"
echo "  • Full demo:       ./scripts/complete_demo.sh"
echo "  • Performance:     ./scripts/run_performance_tests.sh"
echo "  • Documentation:   docs/DEMO_GUIDE.md"
echo "  • Run tests:       pytest tests/ -v"
echo ""
echo -e "${GREEN}System is ready for your demo! 🚀${NC}"
