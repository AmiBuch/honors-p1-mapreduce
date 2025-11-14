#!/usr/bin/env python3
"""
Test Data Generator for MapReduce

Generates various sizes and types of test data for benchmarking
and testing the MapReduce system.
"""

import random
import string
import argparse
from pathlib import Path


def generate_word_count_data(output_file, num_lines, words_per_line=10):
    """Generate random text data for word count testing."""
    common_words = [
        'the', 'be', 'to', 'of', 'and', 'a', 'in', 'that', 'have', 'it',
        'for', 'not', 'on', 'with', 'he', 'as', 'you', 'do', 'at', 'this',
        'but', 'his', 'by', 'from', 'they', 'we', 'say', 'her', 'she', 'or',
        'an', 'will', 'my', 'one', 'all', 'would', 'there', 'their', 'what',
        'so', 'up', 'out', 'if', 'about', 'who', 'get', 'which', 'go', 'me',
        'data', 'processing', 'mapreduce', 'distributed', 'system', 'cluster',
        'compute', 'parallel', 'scale', 'performance', 'algorithm', 'test'
    ]
    
    print(f"Generating word count data: {num_lines} lines...")
    
    with open(output_file, 'w') as f:
        for i in range(num_lines):
            words = [random.choice(common_words) for _ in range(words_per_line)]
            line = ' '.join(words)
            f.write(f"{line}\n")
            
            if (i + 1) % 100000 == 0:
                print(f"  Written {i + 1} lines...")
    
    size_mb = Path(output_file).stat().st_size / (1024 * 1024)
    print(f"✓ Created {output_file} ({size_mb:.2f} MB)")


def generate_inverted_index_data(output_file, num_docs, words_per_doc=20):
    """Generate document data for inverted index testing."""
    words = [
        'algorithm', 'database', 'network', 'security', 'cloud', 'server',
        'client', 'protocol', 'encryption', 'authentication', 'authorization',
        'scalability', 'performance', 'latency', 'throughput', 'bandwidth',
        'distributed', 'concurrent', 'parallel', 'asynchronous', 'synchronous',
        'architecture', 'infrastructure', 'deployment', 'monitoring', 'logging'
    ]
    
    print(f"Generating inverted index data: {num_docs} documents...")
    
    with open(output_file, 'w') as f:
        for i in range(num_docs):
            doc_id = f"doc_{i+1:06d}"
            doc_words = [random.choice(words) for _ in range(words_per_doc)]
            content = ' '.join(doc_words)
            f.write(f"{doc_id}: {content}\n")
            
            if (i + 1) % 10000 == 0:
                print(f"  Written {i + 1} documents...")
    
    size_mb = Path(output_file).stat().st_size / (1024 * 1024)
    print(f"✓ Created {output_file} ({size_mb:.2f} MB)")


def generate_log_data(output_file, num_lines):
    """Generate log file data for grep/filter testing."""
    log_levels = ['INFO', 'WARN', 'ERROR', 'DEBUG']
    components = ['api', 'database', 'cache', 'worker', 'scheduler', 'monitor']
    messages = [
        'Request processed successfully',
        'Connection timeout occurred',
        'Cache miss for key',
        'Task completed in {}ms',
        'Error processing request',
        'Database query executed',
        'Authentication failed',
        'Rate limit exceeded',
        'Service unavailable',
        'Resource not found'
    ]
    
    print(f"Generating log data: {num_lines} lines...")
    
    with open(output_file, 'w') as f:
        for i in range(num_lines):
            timestamp = f"2024-11-{random.randint(1, 30):02d} {random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"
            level = random.choice(log_levels)
            component = random.choice(components)
            message = random.choice(messages)
            
            # Add timing to some messages
            if '{}' in message:
                message = message.format(random.randint(10, 5000))
            
            log_line = f"{timestamp} [{level}] {component}: {message}"
            f.write(f"{log_line}\n")
            
            if (i + 1) % 100000 == 0:
                print(f"  Written {i + 1} lines...")
    
    size_mb = Path(output_file).stat().st_size / (1024 * 1024)
    print(f"✓ Created {output_file} ({size_mb:.2f} MB)")


def generate_csv_data(output_file, num_rows):
    """Generate CSV data for analysis testing."""
    print(f"Generating CSV data: {num_rows} rows...")
    
    categories = ['Electronics', 'Clothing', 'Food', 'Books', 'Sports', 'Home']
    regions = ['North', 'South', 'East', 'West', 'Central']
    
    with open(output_file, 'w') as f:
        # Header
        f.write("id,category,region,quantity,price,timestamp\n")
        
        for i in range(num_rows):
            row_id = i + 1
            category = random.choice(categories)
            region = random.choice(regions)
            quantity = random.randint(1, 100)
            price = round(random.uniform(10, 1000), 2)
            timestamp = f"2024-11-{random.randint(1, 30):02d}"
            
            f.write(f"{row_id},{category},{region},{quantity},{price},{timestamp}\n")
            
            if (i + 1) % 100000 == 0:
                print(f"  Written {i + 1} rows...")
    
    size_mb = Path(output_file).stat().st_size / (1024 * 1024)
    print(f"✓ Created {output_file} ({size_mb:.2f} MB)")


def main():
    parser = argparse.ArgumentParser(
        description='Generate test data for MapReduce benchmarking'
    )
    
    parser.add_argument(
        '--type',
        choices=['wordcount', 'inverted', 'logs', 'csv', 'all'],
        default='all',
        help='Type of data to generate'
    )
    
    parser.add_argument(
        '--size',
        choices=['small', 'medium', 'large', 'xlarge'],
        default='medium',
        help='Size of data to generate'
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        default='data/input',
        help='Output directory for generated files'
    )
    
    args = parser.parse_args()
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Define sizes
    sizes = {
        'small': {
            'lines': 1000,
            'docs': 500,
            'rows': 1000
        },
        'medium': {
            'lines': 100000,
            'docs': 50000,
            'rows': 100000
        },
        'large': {
            'lines': 1000000,
            'docs': 500000,
            'rows': 1000000
        },
        'xlarge': {
            'lines': 5000000,
            'docs': 2000000,
            'rows': 5000000
        }
    }
    
    size_config = sizes[args.size]
    
    print(f"\nGenerating {args.size} test data...\n")
    
    # Generate requested data types
    if args.type in ['wordcount', 'all']:
        output_file = output_dir / f'wordcount_{args.size}.txt'
        generate_word_count_data(output_file, size_config['lines'])
    
    if args.type in ['inverted', 'all']:
        output_file = output_dir / f'documents_{args.size}.txt'
        generate_inverted_index_data(output_file, size_config['docs'])
    
    if args.type in ['logs', 'all']:
        output_file = output_dir / f'logs_{args.size}.txt'
        generate_log_data(output_file, size_config['lines'])
    
    if args.type in ['csv', 'all']:
        output_file = output_dir / f'sales_{args.size}.csv'
        generate_csv_data(output_file, size_config['rows'])
    
    print(f"\n✓ All test data generated in {output_dir}/")
    print("\nYou can now use these files with MapReduce:")
    print(f"  python client/client.py upload {output_dir}/wordcount_{args.size}.txt /data/input/")


if __name__ == '__main__':
    main()
