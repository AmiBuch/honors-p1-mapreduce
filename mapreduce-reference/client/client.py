#!/usr/bin/env python3
import grpc
import argparse
import sys
import os
import time
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proto import mapreduce_pb2
from proto import mapreduce_pb2_grpc


class MapReduceClient:
    def __init__(self, coordinator_host='localhost', coordinator_port='50051'):
        channel = grpc.insecure_channel(f'{coordinator_host}:{coordinator_port}')
        self.stub = mapreduce_pb2_grpc.CoordinatorStub(channel)

    def upload(self, local_path, remote_path):
        """Upload a file to shared storage."""
        print(f"Uploading {local_path} to {remote_path}...")
        
        # Ensure local file exists
        if not os.path.exists(local_path):
            print(f"Error: Local file {local_path} not found")
            return False
        
        # Create parent directory if needed
        remote_dir = os.path.dirname(remote_path)
        os.makedirs(remote_dir, exist_ok=True)
        
        # Copy file
        with open(local_path, 'r') as src:
            with open(remote_path, 'w') as dst:
                dst.write(src.read())
        
        print(f"✓ Upload complete")
        return True

    def submit(self, input_path, output_path, mapper_file, reducer_file, num_maps, num_reduces):
        """Submit a MapReduce job."""
        print(f"Submitting job...")
        print(f"  Input: {input_path}")
        print(f"  Output: {output_path}")
        print(f"  Mapper: {mapper_file}")
        print(f"  Reducer: {reducer_file}")
        print(f"  Maps: {num_maps}, Reduces: {num_reduces}")
        
        # Read mapper and reducer code
        try:
            with open(mapper_file, 'rb') as f:
                mapper_code = f.read()
            with open(reducer_file, 'rb') as f:
                reducer_code = f.read()
        except FileNotFoundError as e:
            print(f"Error: Could not read file: {e}")
            return None
        
        # Submit job
        try:
            response = self.stub.SubmitJob(
                mapreduce_pb2.SubmitJobRequest(
                    input_path=input_path,
                    output_path=output_path,
                    mapper_code=mapper_code,
                    reducer_code=reducer_code,
                    num_maps=num_maps,
                    num_reduces=num_reduces
                )
            )
            
            if response.success:
                print(f"✓ Job submitted successfully!")
                print(f"  Job ID: {response.job_id}")
                print(f"  Message: {response.message}")
                return response.job_id
            else:
                print(f"✗ Job submission failed: {response.message}")
                return None
                
        except grpc.RpcError as e:
            print(f"✗ RPC Error: {e}")
            return None

    def status(self, job_id, follow=False):
        """Get job status."""
        try:
            while True:
                response = self.stub.GetJobStatus(
                    mapreduce_pb2.GetJobStatusRequest(job_id=job_id)
                )
                
                if response.status == "NOT_FOUND":
                    print(f"✗ Job {job_id} not found")
                    return
                
                # Clear line if following
                if follow:
                    print("\r", end="")
                
                print(f"Job {job_id}: {response.status}")
                print(f"  Map tasks: {response.map_progress}/{response.total_maps}")
                print(f"  Reduce tasks: {response.reduce_progress}/{response.total_reduces}")
                
                if response.status in ["COMPLETED", "FAILED"]:
                    break
                
                if not follow:
                    break
                
                time.sleep(2)
                
        except grpc.RpcError as e:
            print(f"✗ RPC Error: {e}")
        except KeyboardInterrupt:
            print("\nStopped following job status")

    def results(self, output_path, limit=None):
        """Display job results."""
        print(f"Results from {output_path}:")
        print("=" * 60)
        
        # Find all output files
        output_files = sorted(Path(output_path).parent.glob("reduce-*.txt"))
        
        if not output_files:
            print("No output files found")
            return
        
        line_count = 0
        for output_file in output_files:
            with open(output_file, 'r') as f:
                for line in f:
                    print(line.rstrip())
                    line_count += 1
                    if limit and line_count >= limit:
                        print(f"\n... (showing first {limit} lines)")
                        return
        
        print(f"\n{line_count} total results")


def main():
    parser = argparse.ArgumentParser(description='MapReduce Client')
    parser.add_argument('--host', default='localhost', help='Coordinator host')
    parser.add_argument('--port', default='50051', help='Coordinator port')
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Upload command
    upload_parser = subparsers.add_parser('upload', help='Upload file to shared storage')
    upload_parser.add_argument('local_path', help='Local file path')
    upload_parser.add_argument('remote_path', help='Remote path in shared storage')
    
    # Submit command
    submit_parser = subparsers.add_parser('submit', help='Submit a MapReduce job')
    submit_parser.add_argument('--input', required=True, help='Input file path')
    submit_parser.add_argument('--output', required=True, help='Output directory path')
    submit_parser.add_argument('--mapper', required=True, help='Mapper Python file')
    submit_parser.add_argument('--reducer', required=True, help='Reducer Python file')
    submit_parser.add_argument('--num-maps', type=int, default=4, help='Number of map tasks')
    submit_parser.add_argument('--num-reduces', type=int, default=2, help='Number of reduce tasks')
    submit_parser.add_argument('--follow', action='store_true', help='Follow job status until completion')
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Check job status')
    status_parser.add_argument('job_id', help='Job ID to check')
    status_parser.add_argument('--follow', action='store_true', help='Follow status until completion')
    
    # Results command
    results_parser = subparsers.add_parser('results', help='Display job results')
    results_parser.add_argument('output_path', help='Output directory path')
    results_parser.add_argument('--limit', type=int, help='Limit number of lines to display')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    client = MapReduceClient(args.host, args.port)
    
    if args.command == 'upload':
        client.upload(args.local_path, args.remote_path)
    
    elif args.command == 'submit':
        job_id = client.submit(
            args.input,
            args.output,
            args.mapper,
            args.reducer,
            args.num_maps,
            args.num_reduces
        )
        if job_id and args.follow:
            print("\nFollowing job status...")
            client.status(job_id, follow=True)
    
    elif args.command == 'status':
        client.status(args.job_id, args.follow)
    
    elif args.command == 'results':
        client.results(args.output_path, args.limit)


if __name__ == '__main__':
    main()
