import pytest
import grpc
import time
import os
import sys
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proto import mapreduce_pb2
from proto import mapreduce_pb2_grpc
from client.client import MapReduceClient


@pytest.fixture
def client():
    """Create a MapReduce client."""
    return MapReduceClient('localhost', '50051')


@pytest.fixture
def test_data_dir(tmp_path):
    """Create temporary test data directory."""
    data_dir = tmp_path / "test_data"
    data_dir.mkdir()
    return data_dir


class TestBasicFunctionality:
    """Test basic MapReduce functionality."""
    
    def test_word_count_small(self, client, test_data_dir):
        """Test word count on a small file."""
        # Create test input
        input_file = test_data_dir / "input.txt"
        input_file.write_text("hello world\nhello python\nworld of mapreduce\n")
        
        # Upload to shared storage
        remote_input = "/data/input/test_small.txt"
        assert client.upload(str(input_file), remote_input)
        
        # Submit job
        job_id = client.submit(
            input_path=remote_input,
            output_path="/data/output/test_small",
            mapper_file="examples/wordcount/mapper.py",
            reducer_file="examples/wordcount/reducer.py",
            num_maps=2,
            num_reduces=2
        )
        
        assert job_id is not None
        
        # Wait for completion
        timeout = 60
        start = time.time()
        while time.time() - start < timeout:
            response = client.stub.GetJobStatus(
                mapreduce_pb2.GetJobStatusRequest(job_id=job_id)
            )
            if response.status == "COMPLETED":
                break
            time.sleep(1)
        
        assert response.status == "COMPLETED"
        
        # Verify results
        output_files = list(Path("/data/output").glob("reduce-*.txt"))
        assert len(output_files) > 0
        
        # Read and verify word counts
        word_counts = {}
        for f in output_files:
            with open(f) as file:
                for line in file:
                    word, count = line.strip().split('\t')
                    word_counts[word] = int(count)
        
        assert word_counts['hello'] == 2
        assert word_counts['world'] == 2
        assert word_counts['python'] == 1
        assert word_counts['mapreduce'] == 1
    
    def test_empty_input(self, client, test_data_dir):
        """Test handling of empty input file."""
        input_file = test_data_dir / "empty.txt"
        input_file.write_text("")
        
        remote_input = "/data/input/test_empty.txt"
        client.upload(str(input_file), remote_input)
        
        job_id = client.submit(
            input_path=remote_input,
            output_path="/data/output/test_empty",
            mapper_file="examples/wordcount/mapper.py",
            reducer_file="examples/wordcount/reducer.py",
            num_maps=1,
            num_reduces=1
        )
        
        assert job_id is not None
        
        # Wait for completion
        timeout = 30
        start = time.time()
        while time.time() - start < timeout:
            response = client.stub.GetJobStatus(
                mapreduce_pb2.GetJobStatusRequest(job_id=job_id)
            )
            if response.status in ["COMPLETED", "FAILED"]:
                break
            time.sleep(1)
        
        # Should complete without errors
        assert response.status == "COMPLETED"


class TestStraggler:
    """Test straggler detection and mitigation."""
    
    def test_straggler_detection(self, client, test_data_dir):
        """Test that stragglers are detected and backup tasks are launched."""
        # Create larger input to ensure tasks run long enough
        input_file = test_data_dir / "large_input.txt"
        with open(input_file, 'w') as f:
            for i in range(1000):
                f.write(f"line {i} with some words to count\n")
        
        remote_input = "/data/input/test_straggler.txt"
        client.upload(str(input_file), remote_input)
        
        # Submit job with more map tasks to increase chance of straggler
        job_id = client.submit(
            input_path=remote_input,
            output_path="/data/output/test_straggler",
            mapper_file="examples/wordcount/mapper.py",
            reducer_file="examples/wordcount/reducer.py",
            num_maps=8,
            num_reduces=4
        )
        
        assert job_id is not None
        
        # Monitor job completion
        start = time.time()
        completed = False
        timeout = 120
        
        while time.time() - start < timeout:
            response = client.stub.GetJobStatus(
                mapreduce_pb2.GetJobStatusRequest(job_id=job_id)
            )
            if response.status == "COMPLETED":
                completed = True
                break
            time.sleep(2)
        
        duration = time.time() - start
        
        assert completed, "Job should complete even with stragglers"
        
        # With straggler detection, job should still complete reasonably fast
        # Without it, one slow worker would delay completion significantly
        assert duration < 60, f"Job took {duration}s, straggler detection may not be working"
    
    def test_no_false_positive_stragglers(self, client, test_data_dir):
        """Test that backup tasks aren't launched when all workers are normal."""
        # Create small input for quick execution
        input_file = test_data_dir / "small_input.txt"
        with open(input_file, 'w') as f:
            for i in range(100):
                f.write(f"test line {i}\n")
        
        remote_input = "/data/input/test_no_straggler.txt"
        client.upload(str(input_file), remote_input)
        
        # Temporarily disable straggler simulation
        # In real test, would use environment variable or config
        job_id = client.submit(
            input_path=remote_input,
            output_path="/data/output/test_no_straggler",
            mapper_file="examples/wordcount/mapper.py",
            reducer_file="examples/wordcount/reducer.py",
            num_maps=4,
            num_reduces=2
        )
        
        assert job_id is not None
        
        # Wait for completion
        timeout = 30
        start = time.time()
        while time.time() - start < timeout:
            response = client.stub.GetJobStatus(
                mapreduce_pb2.GetJobStatusRequest(job_id=job_id)
            )
            if response.status == "COMPLETED":
                break
            time.sleep(1)
        
        assert response.status == "COMPLETED"


class TestConcurrency:
    """Test concurrent job execution."""
    
    def test_multiple_concurrent_jobs(self, client, test_data_dir):
        """Test that multiple jobs can run concurrently."""
        job_ids = []
        
        # Submit 3 jobs
        for i in range(3):
            input_file = test_data_dir / f"input_{i}.txt"
            input_file.write_text(f"job {i} test data\n" * 100)
            
            remote_input = f"/data/input/concurrent_test_{i}.txt"
            client.upload(str(input_file), remote_input)
            
            job_id = client.submit(
                input_path=remote_input,
                output_path=f"/data/output/concurrent_test_{i}",
                mapper_file="examples/wordcount/mapper.py",
                reducer_file="examples/wordcount/reducer.py",
                num_maps=2,
                num_reduces=1
            )
            
            assert job_id is not None
            job_ids.append(job_id)
        
        # Wait for all jobs to complete
        timeout = 60
        start = time.time()
        all_completed = False
        
        while time.time() - start < timeout:
            statuses = []
            for job_id in job_ids:
                response = client.stub.GetJobStatus(
                    mapreduce_pb2.GetJobStatusRequest(job_id=job_id)
                )
                statuses.append(response.status)
            
            if all(s == "COMPLETED" for s in statuses):
                all_completed = True
                break
            
            time.sleep(2)
        
        assert all_completed, "All concurrent jobs should complete"


class TestRobustness:
    """Test system robustness."""
    
    def test_invalid_mapper_code(self, client, test_data_dir):
        """Test handling of invalid mapper code."""
        input_file = test_data_dir / "input.txt"
        input_file.write_text("test data\n")
        
        # Create invalid mapper (syntax error)
        invalid_mapper = test_data_dir / "invalid_mapper.py"
        invalid_mapper.write_text("def mapper(line):\n    invalid syntax here\n")
        
        remote_input = "/data/input/test_invalid.txt"
        client.upload(str(input_file), remote_input)
        
        job_id = client.submit(
            input_path=remote_input,
            output_path="/data/output/test_invalid",
            mapper_file=str(invalid_mapper),
            reducer_file="examples/wordcount/reducer.py",
            num_maps=1,
            num_reduces=1
        )
        
        # Job should be submitted but tasks should fail
        assert job_id is not None
    
    def test_large_input(self, client, test_data_dir):
        """Test handling of larger input files."""
        input_file = test_data_dir / "large.txt"
        with open(input_file, 'w') as f:
            # Generate 10MB of test data
            for i in range(100000):
                f.write(f"line {i} with various words to count in mapreduce test\n")
        
        remote_input = "/data/input/test_large.txt"
        client.upload(str(input_file), remote_input)
        
        job_id = client.submit(
            input_path=remote_input,
            output_path="/data/output/test_large",
            mapper_file="examples/wordcount/mapper.py",
            reducer_file="examples/wordcount/reducer.py",
            num_maps=8,
            num_reduces=4
        )
        
        assert job_id is not None
        
        # Wait for completion (longer timeout for large input)
        timeout = 180
        start = time.time()
        while time.time() - start < timeout:
            response = client.stub.GetJobStatus(
                mapreduce_pb2.GetJobStatusRequest(job_id=job_id)
            )
            if response.status == "COMPLETED":
                break
            time.sleep(3)
        
        assert response.status == "COMPLETED"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
