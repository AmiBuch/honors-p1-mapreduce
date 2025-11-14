import grpc
import time
import logging
import os
import sys
from collections import defaultdict
from threading import Thread
import importlib.util

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proto import mapreduce_pb2
from proto import mapreduce_pb2_grpc

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class MapReduceWorker:
    def __init__(self, worker_id, coordinator_host, coordinator_port):
        self.worker_id = worker_id
        self.coordinator_host = coordinator_host
        self.coordinator_port = coordinator_port
        self.current_task = None
        self.simulate_straggler = os.getenv('SIMULATE_STRAGGLER', 'false').lower() == 'true'
        
        # Connect to coordinator
        channel = grpc.insecure_channel(f'{coordinator_host}:{coordinator_port}')
        self.stub = mapreduce_pb2_grpc.CoordinatorStub(channel)
        
        logger.info(f"Worker {worker_id} initialized (straggler mode: {self.simulate_straggler})")

    def run(self):
        """Main worker loop."""
        # Start heartbeat thread
        Thread(target=self._send_heartbeats, daemon=True).start()
        
        while True:
            try:
                # Request a task
                response = self.stub.GetTask(
                    mapreduce_pb2.GetTaskRequest(worker_id=self.worker_id)
                )
                
                if response.task_type == "NONE":
                    # No tasks available, wait and retry
                    time.sleep(2)
                    continue
                
                # Execute the task
                self.current_task = response.task_id
                logger.info(f"Worker {self.worker_id} got task {response.task_id} of type {response.task_type}")
                
                success = False
                error_msg = ""
                
                try:
                    if response.task_type == "MAP":
                        self._execute_map_task(response)
                        success = True
                    elif response.task_type == "REDUCE":
                        self._execute_reduce_task(response)
                        success = True
                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"Task {response.task_id} failed: {error_msg}")
                
                # Report completion
                self.stub.ReportTaskComplete(
                    mapreduce_pb2.ReportTaskCompleteRequest(
                        worker_id=self.worker_id,
                        task_id=response.task_id,
                        success=success,
                        error_message=error_msg
                    )
                )
                
                self.current_task = None
                
            except grpc.RpcError as e:
                logger.error(f"RPC error: {e}")
                time.sleep(5)
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                time.sleep(5)

    def _execute_map_task(self, task):
        """Execute a map task."""
        # Simulate straggler by adding delay
        if self.simulate_straggler:
            logger.warning(f"SIMULATING STRAGGLER: Adding 10s delay")
            time.sleep(10)
        
        # Load mapper function
        mapper = self._load_function(task.mapper_code, 'mapper')
        
        # Read input file
        with open(task.input_file, 'r') as f:
            lines = f.readlines()
        
        # Process each line through mapper
        intermediate_data = defaultdict(list)
        
        for line in lines:
            try:
                for key, value in mapper(line):
                    # Partition by hash
                    reduce_task = hash(key) % task.num_reduces
                    intermediate_data[reduce_task].append((key, value))
            except Exception as e:
                logger.warning(f"Mapper error on line: {e}")
        
        # Write intermediate files
        intermediate_dir = f"/data/intermediate/{task.job_id}"
        os.makedirs(intermediate_dir, exist_ok=True)
        
        for reduce_task_num, kv_pairs in intermediate_data.items():
            output_file = f"{intermediate_dir}/map-{task.map_task_number}-reduce-{reduce_task_num}.pb"
            
            # Create protobuf message
            kv_list = mapreduce_pb2.KeyValueList()
            for key, value in kv_pairs:
                kv = kv_list.pairs.add()
                kv.key = str(key)
                kv.value = str(value)
            
            # Write to file
            with open(output_file, 'wb') as f:
                f.write(kv_list.SerializeToString())
        
        logger.info(f"Map task {task.task_id} completed, processed {len(lines)} lines")

    def _execute_reduce_task(self, task):
        """Execute a reduce task."""
        # Simulate straggler by adding delay
        if self.simulate_straggler:
            logger.warning(f"SIMULATING STRAGGLER: Adding 10s delay")
            time.sleep(10)
        
        # Load reducer function
        reducer = self._load_function(task.reducer_code, 'reducer')
        
        # Read all intermediate files for this reduce task
        intermediate_dir = f"/data/intermediate/{task.job_id}"
        all_pairs = defaultdict(list)
        
        for map_task_num in range(task.num_maps):
            input_file = f"{intermediate_dir}/map-{map_task_num}-reduce-{task.reduce_task_number}.pb"
            
            if not os.path.exists(input_file):
                continue
            
            # Read protobuf file
            with open(input_file, 'rb') as f:
                kv_list = mapreduce_pb2.KeyValueList()
                kv_list.ParseFromString(f.read())
                
                for kv in kv_list.pairs:
                    all_pairs[kv.key].append(kv.value)
        
        # Run reducer on each key
        output_dir = "/data/output"
        os.makedirs(output_dir, exist_ok=True)
        output_file = f"{output_dir}/reduce-{task.reduce_task_number}.txt"
        
        with open(output_file, 'w') as f:
            for key in sorted(all_pairs.keys()):
                values = all_pairs[key]
                try:
                    for out_key, out_value in reducer(key, values):
                        f.write(f"{out_key}\t{out_value}\n")
                except Exception as e:
                    logger.warning(f"Reducer error on key {key}: {e}")
        
        logger.info(f"Reduce task {task.task_id} completed, processed {len(all_pairs)} keys")

    def _load_function(self, code_bytes, func_name):
        """Dynamically load a function from code bytes."""
        code = code_bytes.decode('utf-8')
        
        # Create a temporary module
        spec = importlib.util.spec_from_loader('user_module', loader=None)
        module = importlib.util.module_from_spec(spec)
        
        # Execute the code in the module's namespace
        exec(code, module.__dict__)
        
        # Return the function
        if not hasattr(module, func_name):
            raise ValueError(f"Function '{func_name}' not found in provided code")
        
        return getattr(module, func_name)

    def _send_heartbeats(self):
        """Send periodic heartbeats to coordinator."""
        while True:
            try:
                self.stub.Heartbeat(
                    mapreduce_pb2.HeartbeatRequest(
                        worker_id=self.worker_id,
                        current_task_id=self.current_task or ""
                    )
                )
            except Exception as e:
                logger.error(f"Heartbeat failed: {e}")
            
            time.sleep(5)


def main():
    worker_id = os.getenv('WORKER_ID', 'worker-unknown')
    coordinator_host = os.getenv('COORDINATOR_HOST', 'localhost')
    coordinator_port = os.getenv('COORDINATOR_PORT', '50051')
    
    worker = MapReduceWorker(worker_id, coordinator_host, coordinator_port)
    worker.run()


if __name__ == '__main__':
    main()
