#!/usr/bin/env python3
"""
Performance evaluation script for MapReduce straggler detection.

This script runs three experiments:
1. Baseline: All workers normal, no stragglers
2. With Straggler, No Detection: One slow worker, detection disabled
3. With Straggler, Detection Enabled: One slow worker, detection enabled

Results are plotted to show the effectiveness of straggler detection.
"""

import subprocess
import time
import json
import sys
import os
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from client.client import MapReduceClient
from proto import mapreduce_pb2


class PerformanceEvaluator:
    def __init__(self):
        self.client = MapReduceClient('localhost', '50051')
        self.results = {
            'baseline': {},
            'straggler_no_detection': {},
            'straggler_with_detection': {}
        }
    
    def prepare_test_data(self, size_mb=100):
        """Generate test data of specified size."""
        print(f"Generating {size_mb}MB test data...")
        
        output_file = Path("/data/input/perf_test.txt")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Generate data
        lines_per_mb = 20000  # Approximate
        total_lines = size_mb * lines_per_mb
        
        with open(output_file, 'w') as f:
            for i in range(total_lines):
                f.write(f"performance test line {i} with multiple words to process in mapreduce\n")
        
        print(f"✓ Generated {total_lines} lines")
        return str(output_file)
    
    def run_job_and_measure(self, experiment_name, enable_straggler=False):
        """Run a job and measure its performance."""
        print(f"\n{'='*60}")
        print(f"Running experiment: {experiment_name}")
        print(f"{'='*60}")
        
        # Configure environment for worker-3
        # In practice, you'd restart docker-compose with different env vars
        # For this script, we assume the configuration is already set
        
        # Submit job
        start_time = time.time()
        
        job_id = self.client.submit(
            input_path="/data/input/perf_test.txt",
            output_path=f"/data/output/perf_{experiment_name}",
            mapper_file="examples/wordcount/mapper.py",
            reducer_file="examples/wordcount/reducer.py",
            num_maps=16,
            num_reduces=8
        )
        
        if not job_id:
            print(f"✗ Failed to submit job")
            return None
        
        print(f"Job ID: {job_id}")
        print("Monitoring progress...")
        
        # Monitor job progress
        task_timings = []
        last_map_progress = 0
        last_reduce_progress = 0
        
        while True:
            response = self.client.stub.GetJobStatus(
                mapreduce_pb2.GetJobStatusRequest(job_id=job_id)
            )
            
            # Track progress changes
            if response.map_progress > last_map_progress:
                last_map_progress = response.map_progress
            if response.reduce_progress > last_reduce_progress:
                last_reduce_progress = response.reduce_progress
            
            # Print progress
            elapsed = time.time() - start_time
            print(f"\r  Elapsed: {elapsed:.1f}s | Maps: {response.map_progress}/{response.total_maps} | "
                  f"Reduces: {response.reduce_progress}/{response.total_reduces}   ", end="")
            
            if response.status == "COMPLETED":
                print()  # New line
                break
            elif response.status == "FAILED":
                print(f"\n✗ Job failed")
                return None
            
            time.sleep(1)
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"✓ Job completed in {duration:.2f} seconds")
        
        # Store results
        result = {
            'job_id': job_id,
            'duration': duration,
            'num_maps': response.total_maps,
            'num_reduces': response.total_reduces
        }
        
        return result
    
    def run_all_experiments(self):
        """Run all three experiments."""
        # Prepare test data
        self.prepare_test_data(size_mb=50)
        
        print("\n" + "="*60)
        print("EXPERIMENT 1: Baseline (No Stragglers)")
        print("="*60)
        print("Instructions: Ensure SIMULATE_STRAGGLER=false for all workers")
        input("Press Enter when ready...")
        
        self.results['baseline'] = self.run_job_and_measure('baseline', enable_straggler=False)
        
        print("\n" + "="*60)
        print("EXPERIMENT 2: Straggler with Detection Disabled")
        print("="*60)
        print("Instructions: Set SIMULATE_STRAGGLER=true for worker-3")
        print("             Disable straggler detection in coordinator")
        input("Press Enter when ready...")
        
        self.results['straggler_no_detection'] = self.run_job_and_measure(
            'straggler_no_detection', enable_straggler=True
        )
        
        print("\n" + "="*60)
        print("EXPERIMENT 3: Straggler with Detection Enabled")
        print("="*60)
        print("Instructions: Keep SIMULATE_STRAGGLER=true for worker-3")
        print("             Enable straggler detection in coordinator")
        input("Press Enter when ready...")
        
        self.results['straggler_with_detection'] = self.run_job_and_measure(
            'straggler_with_detection', enable_straggler=True
        )
        
        # Save results
        self.save_results()
        
        # Generate plots
        self.generate_plots()
    
    def save_results(self):
        """Save results to JSON file."""
        output_file = "performance_results.json"
        with open(output_file, 'w') as f:
            json.dump(self.results, f, indent=2)
        print(f"\n✓ Results saved to {output_file}")
    
    def generate_plots(self):
        """Generate performance comparison plots."""
        print("\nGenerating plots...")
        
        # Extract durations
        experiments = ['baseline', 'straggler_no_detection', 'straggler_with_detection']
        labels = ['Baseline\n(No Stragglers)', 'With Straggler\n(No Detection)', 
                  'With Straggler\n(Detection Enabled)']
        durations = [self.results[exp]['duration'] for exp in experiments if self.results[exp]]
        
        # Create figure with multiple subplots
        fig, axes = plt.subplots(1, 2, figsize=(14, 6))
        
        # Plot 1: Bar chart of completion times
        ax1 = axes[0]
        colors = ['#2ecc71', '#e74c3c', '#3498db']
        bars = ax1.bar(labels, durations, color=colors, alpha=0.8, edgecolor='black')
        
        # Add value labels on bars
        for bar, duration in zip(bars, durations):
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height,
                    f'{duration:.1f}s',
                    ha='center', va='bottom', fontweight='bold')
        
        ax1.set_ylabel('Job Completion Time (seconds)', fontsize=12, fontweight='bold')
        ax1.set_title('MapReduce Job Performance Comparison', fontsize=14, fontweight='bold')
        ax1.grid(axis='y', alpha=0.3)
        
        # Calculate speedup
        if len(durations) >= 3:
            baseline_time = durations[0]
            straggler_no_detect = durations[1]
            straggler_with_detect = durations[2]
            
            speedup = straggler_no_detect / straggler_with_detect
            
            # Add speedup annotation
            ax1.text(0.5, 0.95, f'Speedup with Detection: {speedup:.2f}x',
                    transform=ax1.transAxes,
                    fontsize=12, fontweight='bold',
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5),
                    ha='center', va='top')
        
        # Plot 2: Relative performance
        ax2 = axes[1]
        if len(durations) >= 3:
            baseline = durations[0]
            relative_times = [d / baseline for d in durations]
            
            bars2 = ax2.bar(labels, relative_times, color=colors, alpha=0.8, edgecolor='black')
            
            # Add value labels
            for bar, rel_time in zip(bars2, relative_times):
                height = bar.get_height()
                ax2.text(bar.get_x() + bar.get_width()/2., height,
                        f'{rel_time:.2f}x',
                        ha='center', va='bottom', fontweight='bold')
            
            ax2.axhline(y=1.0, color='red', linestyle='--', linewidth=2, alpha=0.7,
                       label='Baseline Performance')
            ax2.set_ylabel('Relative Time (vs Baseline)', fontsize=12, fontweight='bold')
            ax2.set_title('Relative Performance Impact', fontsize=14, fontweight='bold')
            ax2.legend()
            ax2.grid(axis='y', alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('performance_comparison.png', dpi=300, bbox_inches='tight')
        print(f"✓ Plots saved to performance_comparison.png")
        
        # Generate summary statistics
        self.print_summary()
    
    def print_summary(self):
        """Print summary statistics."""
        print("\n" + "="*60)
        print("PERFORMANCE SUMMARY")
        print("="*60)
        
        for exp_name, result in self.results.items():
            if result:
                print(f"\n{exp_name.replace('_', ' ').title()}:")
                print(f"  Duration: {result['duration']:.2f} seconds")
                print(f"  Maps: {result['num_maps']}")
                print(f"  Reduces: {result['num_reduces']}")
        
        if all(self.results.values()):
            baseline = self.results['baseline']['duration']
            no_detect = self.results['straggler_no_detection']['duration']
            with_detect = self.results['straggler_with_detection']['duration']
            
            print(f"\nKey Findings:")
            print(f"  • Straggler overhead (no detection): {(no_detect/baseline - 1)*100:.1f}% slower")
            print(f"  • Detection improvement: {(no_detect/with_detect):.2f}x speedup")
            print(f"  • Detection overhead vs baseline: {(with_detect/baseline - 1)*100:.1f}%")


def main():
    print("""
╔════════════════════════════════════════════════════════════════╗
║         MapReduce Straggler Detection Performance Test         ║
╚════════════════════════════════════════════════════════════════╝

This script will run three experiments to evaluate straggler detection:

1. Baseline: All workers normal
2. With straggler, detection disabled
3. With straggler, detection enabled

Make sure the MapReduce cluster is running before starting.
    """)
    
    input("Press Enter to begin...")
    
    evaluator = PerformanceEvaluator()
    evaluator.run_all_experiments()
    
    print("\n✓ Performance evaluation complete!")
    print("Check performance_comparison.png for visualizations")


if __name__ == '__main__':
    main()
