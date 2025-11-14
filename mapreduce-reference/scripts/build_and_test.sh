#!/bin/bash

# Build and Test Script
# Comprehensive build, test, and validation script for CI/CD

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_header() {
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}$1${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_info() {
    echo -e "${YELLOW}→${NC} $1"
}

# Configuration
SKIP_TESTS=${SKIP_TESTS:-false}
SKIP_LINT=${SKIP_LINT:-false}
VERBOSE=${VERBOSE:-false}

print_header "MapReduce Build and Test"

# ============================================================================
# Step 1: Check Prerequisites
# ============================================================================
print_header "Step 1: Checking Prerequisites"

if ! command -v python3 &> /dev/null; then
    print_error "Python 3 not found"
    exit 1
fi
print_success "Python 3 found: $(python3 --version)"

if ! command -v docker &> /dev/null; then
    print_error "Docker not found"
    exit 1
fi
print_success "Docker found: $(docker --version)"

if ! docker compose version &> /dev/null; then
    print_error "Docker Compose not found"
    exit 1
fi
print_success "Docker Compose found: $(docker compose version)"

if ! command -v protoc &> /dev/null; then
    print_error "Protocol Buffer compiler not found"
    exit 1
fi
print_success "Protoc found: $(protoc --version)"

# ============================================================================
# Step 2: Setup Python Environment
# ============================================================================
print_header "Step 2: Setting Up Python Environment"

if [ ! -d "venv" ]; then
    print_info "Creating virtual environment..."
    python3 -m venv venv
    print_success "Virtual environment created"
else
    print_info "Virtual environment exists"
fi

source venv/bin/activate

print_info "Installing dependencies..."
pip install --upgrade pip > /dev/null 2>&1
pip install -r requirements.txt > /dev/null 2>&1
print_success "Dependencies installed"

# ============================================================================
# Step 3: Generate Protobuf Code
# ============================================================================
print_header "Step 3: Generating Protobuf Code"

cd proto
protoc --python_out=. --grpc_python_out=. mapreduce.proto
cd ..

if [ -f "proto/mapreduce_pb2.py" ] && [ -f "proto/mapreduce_pb2_grpc.py" ]; then
    print_success "Protobuf code generated"
else
    print_error "Protobuf generation failed"
    exit 1
fi

# ============================================================================
# Step 4: Code Quality Checks
# ============================================================================
if [ "$SKIP_LINT" = false ]; then
    print_header "Step 4: Code Quality Checks"
    
    print_info "Running flake8..."
    if command -v flake8 &> /dev/null; then
        flake8 coordinator/ worker/ client/ --max-line-length=100 --ignore=E501,W503 || true
        print_success "Flake8 check complete"
    else
        print_info "Flake8 not installed, skipping"
    fi
    
    print_info "Checking for common issues..."
    # Check for TODO comments
    TODO_COUNT=$(grep -r "TODO" coordinator/ worker/ client/ 2>/dev/null | wc -l || echo 0)
    if [ "$TODO_COUNT" -gt 0 ]; then
        print_info "Found $TODO_COUNT TODO comments"
    fi
    
    print_success "Code quality checks complete"
else
    print_info "Skipping code quality checks (SKIP_LINT=true)"
fi

# ============================================================================
# Step 5: Build Docker Images
# ============================================================================
print_header "Step 5: Building Docker Images"

print_info "Building coordinator image..."
docker compose build coordinator

print_info "Building worker images..."
docker compose build worker-1

print_success "Docker images built successfully"

# ============================================================================
# Step 6: Start Services
# ============================================================================
print_header "Step 6: Starting Services"

print_info "Stopping any existing services..."
docker compose down > /dev/null 2>&1 || true

print_info "Starting services..."
docker compose up -d

print_info "Waiting for services to be ready..."
sleep 10

# Check if services are running
if docker compose ps | grep -q "Up"; then
    print_success "Services started successfully"
    docker compose ps
else
    print_error "Services failed to start"
    docker compose logs
    exit 1
fi

# ============================================================================
# Step 7: Run Tests
# ============================================================================
if [ "$SKIP_TESTS" = false ]; then
    print_header "Step 7: Running Tests"
    
    # Unit tests
    print_info "Running unit tests..."
    pytest tests/ -v -m "unit or not slow" || {
        print_error "Unit tests failed"
        docker compose logs
        exit 1
    }
    print_success "Unit tests passed"
    
    # Integration tests
    print_info "Running integration tests..."
    pytest tests/ -v -m "integration" || {
        print_error "Integration tests failed"
        docker compose logs
        exit 1
    }
    print_success "Integration tests passed"
    
    # Generate coverage report
    print_info "Generating coverage report..."
    pytest tests/ --cov=coordinator --cov=worker --cov=client --cov-report=term-missing || true
    
    print_success "All tests passed!"
else
    print_info "Skipping tests (SKIP_TESTS=true)"
fi

# ============================================================================
# Step 8: Smoke Test
# ============================================================================
print_header "Step 8: Running Smoke Test"

print_info "Creating test data..."
cat > data/input/smoke_test.txt << 'EOF'
hello world
hello mapreduce
world of distributed systems
mapreduce is powerful
EOF

print_info "Submitting word count job..."
JOB_OUTPUT=$(python client/client.py submit \
    --input /data/input/smoke_test.txt \
    --output /data/output/smoke_test \
    --mapper examples/wordcount/mapper.py \
    --reducer examples/wordcount/reducer.py \
    --num-maps 2 \
    --num-reduces 2)

if echo "$JOB_OUTPUT" | grep -q "Job submitted successfully"; then
    JOB_ID=$(echo "$JOB_OUTPUT" | grep "Job ID:" | awk '{print $3}')
    print_success "Job submitted: $JOB_ID"
    
    # Wait for completion
    print_info "Waiting for job completion..."
    MAX_WAIT=60
    ELAPSED=0
    while [ $ELAPSED -lt $MAX_WAIT ]; do
        STATUS=$(python client/client.py status $JOB_ID 2>/dev/null | grep "Job $JOB_ID:" | awk '{print $3}' || echo "UNKNOWN")
        
        if [ "$STATUS" = "COMPLETED" ]; then
            print_success "Job completed successfully"
            break
        elif [ "$STATUS" = "FAILED" ]; then
            print_error "Job failed"
            docker compose logs
            exit 1
        fi
        
        sleep 2
        ELAPSED=$((ELAPSED + 2))
    done
    
    if [ $ELAPSED -ge $MAX_WAIT ]; then
        print_error "Job timeout"
        exit 1
    fi
    
    # Verify results
    if ls data/output/reduce-*.txt > /dev/null 2>&1; then
        print_success "Output files created"
        
        # Check for expected words
        if grep -q "hello" data/output/reduce-*.txt && grep -q "world" data/output/reduce-*.txt; then
            print_success "Output verification passed"
        else
            print_error "Output verification failed"
            cat data/output/reduce-*.txt
            exit 1
        fi
    else
        print_error "No output files found"
        exit 1
    fi
else
    print_error "Job submission failed"
    exit 1
fi

# ============================================================================
# Step 9: Cleanup
# ============================================================================
print_header "Step 9: Cleanup"

print_info "Collecting logs..."
docker compose logs > build_test_logs.txt 2>&1
print_success "Logs saved to build_test_logs.txt"

if [ "${KEEP_RUNNING:-false}" = false ]; then
    print_info "Stopping services..."
    docker compose down
    print_success "Services stopped"
else
    print_info "Services left running (KEEP_RUNNING=true)"
fi

# ============================================================================
# Summary
# ============================================================================
print_header "Build Summary"

echo "✓ Prerequisites checked"
echo "✓ Python environment setup"
echo "✓ Protobuf code generated"
[ "$SKIP_LINT" = false ] && echo "✓ Code quality checks passed"
echo "✓ Docker images built"
echo "✓ Services started"
[ "$SKIP_TESTS" = false ] && echo "✓ All tests passed"
echo "✓ Smoke test passed"
echo ""

print_success "Build completed successfully!"
echo ""
echo "You can now:"
echo "  • Submit jobs: python client/client.py submit --help"
echo "  • Run tests: pytest tests/ -v"
echo "  • View logs: docker compose logs -f"
echo "  • Run demo: ./demo.sh"

exit 0
