# Makefile for MapReduce Project
# Provides convenient shortcuts for common tasks

.PHONY: help setup proto build up down logs test clean demo

# Default target
help:
	@echo "MapReduce Development Commands:"
	@echo ""
	@echo "  make setup      - Run initial setup (create venv, install deps, generate proto)"
	@echo "  make proto      - Generate protobuf code"
	@echo "  make build      - Build Docker images"
	@echo "  make up         - Start the cluster"
	@echo "  make down       - Stop the cluster"
	@echo "  make logs       - Follow all logs"
	@echo "  make test       - Run test suite"
	@echo "  make test-quick - Run quick tests only"
	@echo "  make perf       - Run performance evaluation"
	@echo "  make demo       - Run complete demo"
	@echo "  make clean      - Clean generated files and data"
	@echo "  make clean-all  - Clean everything including venv"
	@echo ""

# Initial setup
setup:
	@echo "Setting up MapReduce environment..."
	python3 -m venv venv
	. venv/bin/activate && pip install --upgrade pip
	. venv/bin/activate && pip install -r requirements.txt
	$(MAKE) proto
	@echo "✓ Setup complete! Run 'make up' to start the cluster."

# Generate protobuf code
proto:
	@echo "Generating Protocol Buffer code..."
	cd proto && protoc --python_out=. --grpc_python_out=. mapreduce.proto
	@echo "✓ Protobuf code generated"

# Build Docker images
build:
	@echo "Building Docker images..."
	docker compose build
	@echo "✓ Images built"

# Start cluster
up:
	@echo "Starting MapReduce cluster..."
	docker compose up -d
	@sleep 3
	@echo "✓ Cluster started"
	@docker compose ps

# Stop cluster
down:
	@echo "Stopping MapReduce cluster..."
	docker compose down
	@echo "✓ Cluster stopped"

# View logs
logs:
	docker compose logs -f

# View coordinator logs only
logs-coordinator:
	docker compose logs -f coordinator

# View worker logs
logs-workers:
	docker compose logs -f worker-1 worker-2 worker-3 worker-4

# Run all tests
test:
	@echo "Running test suite..."
	. venv/bin/activate && pytest tests/ -v

# Run quick tests (skip slow ones)
test-quick:
	@echo "Running quick tests..."
	. venv/bin/activate && pytest tests/ -v -m "not slow"

# Run unit tests only
test-unit:
	@echo "Running unit tests..."
	. venv/bin/activate && pytest tests/ -v -m unit

# Run integration tests only
test-integration:
	@echo "Running integration tests..."
	. venv/bin/activate && pytest tests/ -v -m integration

# Run straggler tests
test-straggler:
	@echo "Running straggler detection tests..."
	. venv/bin/activate && pytest tests/ -v -m straggler

# Run performance evaluation
perf:
	@echo "Running performance evaluation..."
	. venv/bin/activate && python tests/performance_evaluation.py

# Submit example word count job
example-wordcount:
	@echo "Submitting word count example..."
	. venv/bin/activate && python client/client.py submit \
		--input /data/input/shakespeare.txt \
		--output /data/output/wordcount \
		--mapper examples/wordcount/mapper.py \
		--reducer examples/wordcount/reducer.py \
		--num-maps 4 \
		--num-reduces 2 \
		--follow

# Submit inverted index job
example-index:
	@echo "Submitting inverted index example..."
	@echo "doc_1: the quick brown fox" > data/input/docs.txt
	@echo "doc_2: the lazy brown dog" >> data/input/docs.txt
	@echo "doc_3: quick brown animals" >> data/input/docs.txt
	. venv/bin/activate && python client/client.py submit \
		--input /data/input/docs.txt \
		--output /data/output/index \
		--mapper examples/inverted_index/mapper.py \
		--reducer examples/inverted_index/reducer.py \
		--num-maps 2 \
		--num-reduces 2 \
		--follow

# Run complete demo
demo:
	@echo "Running MapReduce demo..."
	$(MAKE) down
	$(MAKE) up
	@sleep 5
	$(MAKE) example-wordcount
	@echo ""
	@echo "=== Results ==="
	. venv/bin/activate && python client/client.py results /data/output/wordcount --limit 20
	@echo ""
	@echo "✓ Demo complete!"

# Clean generated files and data
clean:
	@echo "Cleaning generated files..."
	rm -rf data/intermediate/*
	rm -rf data/output/*
	rm -f performance_results.json
	rm -f performance_comparison.png
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	@echo "✓ Cleaned"

# Clean everything including venv
clean-all: clean down
	@echo "Cleaning everything..."
	rm -rf venv/
	rm -rf proto/*_pb2.py proto/*_pb2_grpc.py
	docker compose down -v
	@echo "✓ Deep clean complete"

# Restart cluster
restart: down up

# Check cluster status
status:
	@echo "Cluster Status:"
	@docker compose ps
	@echo ""
	@echo "Resource Usage:"
	@docker stats --no-stream

# Enter coordinator shell
shell-coordinator:
	docker compose exec coordinator /bin/bash

# Enter worker shell
shell-worker:
	docker compose exec worker-1 /bin/bash

# Generate test data
generate-data:
	@echo "Generating test data..."
	@python3 << 'EOF'
# Generate various sizes of test data
sizes = {
    'small': 1000,      # 1K lines
    'medium': 100000,   # 100K lines  
    'large': 1000000,   # 1M lines
}

for name, lines in sizes.items():
    filename = f'data/input/test_{name}.txt'
    with open(filename, 'w') as f:
        for i in range(lines):
            f.write(f'line {i} with test data for mapreduce processing\n')
    print(f'Generated {filename} ({lines} lines)')
EOF
	@echo "✓ Test data generated"

# Run linting
lint:
	@echo "Running linters..."
	. venv/bin/activate && flake8 coordinator/ worker/ client/ --max-line-length=100 || true
	. venv/bin/activate && pylint coordinator/ worker/ client/ || true

# Format code
format:
	@echo "Formatting code..."
	. venv/bin/activate && black coordinator/ worker/ client/ tests/

# Check code coverage
coverage:
	@echo "Checking test coverage..."
	. venv/bin/activate && pytest tests/ --cov=coordinator --cov=worker --cov=client --cov-report=html
	@echo "Coverage report generated in htmlcov/index.html"
