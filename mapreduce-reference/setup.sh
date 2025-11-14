#!/bin/bash

# MapReduce Setup Script
# Automates the complete setup process

set -e  # Exit on error

echo "╔════════════════════════════════════════════════════════════╗"
echo "║     MapReduce Implementation Setup Script                 ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_info() {
    echo -e "${YELLOW}→${NC} $1"
}

# Check prerequisites
echo "Step 1: Checking prerequisites..."

if ! command -v python3 &> /dev/null; then
    print_error "Python 3 is not installed. Please install Python 3.10 or higher."
    exit 1
fi
print_success "Python 3 found: $(python3 --version)"

if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed. Please install Docker first."
    exit 1
fi
print_success "Docker found: $(docker --version)"

if ! docker compose version &> /dev/null; then
    print_error "Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi
print_success "Docker Compose found: $(docker compose version)"

if ! command -v protoc &> /dev/null; then
    print_error "Protocol Buffer compiler (protoc) is not installed."
    echo "Install it with:"
    echo "  Ubuntu/Debian: sudo apt-get install protobuf-compiler"
    echo "  macOS: brew install protobuf"
    exit 1
fi
print_success "Protoc found: $(protoc --version)"

echo ""

# Create directory structure
echo "Step 2: Creating directory structure..."

mkdir -p proto
mkdir -p coordinator
mkdir -p worker
mkdir -p client
mkdir -p examples/wordcount
mkdir -p examples/data
mkdir -p tests
mkdir -p docs/images
mkdir -p data/{input,intermediate,output}

print_success "Directory structure created"
echo ""

# Create Python virtual environment
echo "Step 4: Setting up Python virtual environment..."

if [ ! -d "venv" ]; then
    python3 -m venv venv
    print_success "Virtual environment created"
else
    print_info "Virtual environment already exists"
fi

# Activate virtual environment
source venv/bin/activate

# Install Python dependencies first (needed for protobuf generation)
echo "Step 5: Installing Python dependencies..."
pip install --upgrade pip > /dev/null 2>&1
pip install -r requirements.txt > /dev/null 2>&1

if [ $? -eq 0 ]; then
    print_success "Python dependencies installed"
else
    print_error "Failed to install Python dependencies"
    exit 1
fi
echo ""

# Generate protobuf code using Python tools
echo "Step 6: Generating Protocol Buffer code with grpcio-tools..."

python -m grpc_tools.protoc \
    -I./proto \
    --python_out=./proto \
    --grpc_python_out=./proto \
    ./proto/mapreduce.proto

if [ $? -eq 0 ]; then
    print_success "Protocol Buffer code generated"
    
    # Fix import in generated grpc file for Docker
    print_info "Fixing protobuf imports for Docker environment..."
    sed -i.bak 's/^import mapreduce_pb2 as mapreduce__pb2/from . import mapreduce_pb2 as mapreduce__pb2/' proto/mapreduce_pb2_grpc.py
    rm -f proto/mapreduce_pb2_grpc.py.bak
    print_success "Imports fixed"
else
    print_error "Failed to generate Protocol Buffer code"
    exit 1
fi

echo ""

# Generate example data
echo "Step 7: Generating example data..."

cat > data/input/sample.txt << 'EOF'
hello world
hello mapreduce
world of distributed systems
mapreduce is powerful
hello again
EOF

print_success "Sample data created at data/input/sample.txt"

# Generate larger test data
print_info "Generating larger test file (5MB)..."
python3 << 'PYTHON_EOF'
with open('data/input/large_test.txt', 'w') as f:
    words = ['hello', 'world', 'mapreduce', 'distributed', 'systems', 'data', 'processing']
    for i in range(100000):
        line = ' '.join(words[j % len(words)] for j in range(i, i+10))
        f.write(f'{line}\n')
print("Generated large_test.txt")
PYTHON_EOF

print_success "Large test file created"
echo ""

# Build Docker images
echo "Step 8: Building Docker images..."

docker compose build

if [ $? -eq 0 ]; then
    print_success "Docker images built successfully"
else
    print_error "Failed to build Docker images"
    exit 1
fi
echo ""

# Start the cluster
echo "Step 9: Starting MapReduce cluster..."

docker compose up -d

if [ $? -eq 0 ]; then
    print_success "Cluster started"
else
    print_error "Failed to start cluster"
    exit 1
fi

# Wait for services to be ready
print_info "Waiting for services to be ready..."
sleep 5

# Check service status
echo ""
echo "Checking service status:"
docker compose ps

echo ""
print_success "Setup complete!"
echo ""
echo "╔════════════════════════════════════════════════════════════╗"
echo "║                      Next Steps                            ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""
echo "1. Activate the virtual environment (if not already active):"
echo "   source venv/bin/activate"
echo ""
echo "2. Run the example word count job:"
echo "   python client/client.py submit \\"
echo "     --input /data/input/sample.txt \\"
echo "     --output /data/output/wordcount \\"
echo "     --mapper examples/wordcount/mapper.py \\"
echo "     --reducer examples/wordcount/reducer.py \\"
echo "     --num-maps 2 \\"
echo "     --num-reduces 2 \\"
echo "     --follow"
echo ""
echo "3. View the results:"
echo "   python client/client.py results /data/output/wordcount"
echo ""
echo "4. Run tests:"
echo "   pytest tests/ -v"
echo ""
echo "5. Run performance evaluation:"
echo "   python tests/performance_evaluation.py"
echo ""
echo "6. View logs:"
echo "   docker compose logs -f"
echo ""
echo "7. Stop the cluster:"
echo "   docker compose down"
echo ""
print_success "Happy MapReducing! 🚀"
