import grpc
from concurrent import futures
import time
import logging
import os
import uuid
from datetime import datetime, timedelta
from collections import defaultdict
from threading import Lock, Thread
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Add proto directory to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'proto'))

from proto import mapreduce_pb2
from proto import mapreduce_pb2_grpc

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class TaskState:
    IDLE = "IDLE"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class JobState:
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class Task:
    def __init__(self, task_id, task_type, job_id):
        self.task_id = task_id
        self.task_type = task_type
        self.job_id = job_id
        self.state = TaskState.IDLE
        self.worker_id = None
        self.start_time = None
        self.end_time = None
        self.backup_task_id = None
        self.is_backup = False


class Job:
    def __init__(self, job_id, request):
        self.job_id = job_id
        self.input_path = request.input_path
        self.output_path = request.output_path
        self.mapper_code = request.mapper_code
        self.reducer_code = request.reducer_code
        self.num_maps = request.num_maps
        self.num_reduces = request.num_reduces
        self.state = JobState.PENDING
        self.map_tasks = {}
        self.reduce_tasks = {}
        self.created_at = datetime.now()


class CoordinatorServicer(mapreduce_pb2_grpc.CoordinatorServicer):
    def __init__(self):
        self.jobs = {}
        self.tasks = {}
        self.lock = Lock()
        self.worker_heartbeats = {}
        
        # Straggler detection parameters
        self.straggler_threshold = 1.5  # Launch backup if task takes 1.5x median
        self.min_tasks_for_baseline = 0.25  # Need 25% complete to establish baseline
        
        # Start background threads
        Thread(target=self._monitor_stragglers, daemon=True).start()
        Thread(target=self._monitor_workers, daemon=True).start()

    def SubmitJob(self, request, context):
        """Submit a new MapReduce job."""
        with self.lock:
            job_id = str(uuid.uuid4())
            job = Job(job_id, request)
            
            # Create map tasks
            input_files = self._split_input(request.input_path, request.num_maps)
            for i, input_file in enumerate(input_files):
                task_id = f"{job_id}-map-{i}"
                task = Task(task_id, "MAP", job_id)
                task.input_file = input_file
                task.map_task_number = i
                self.tasks[task_id] = task
                job.map_tasks[task_id] = task
            
            # Create reduce tasks
            for i in range(request.num_reduces):
                task_id = f"{job_id}-reduce-{i}"
                task = Task(task_id, "REDUCE", job_id)
                task.reduce_task_number = i
                self.tasks[task_id] = task
                job.reduce_tasks[task_id] = task
            
            job.state = JobState.RUNNING
            self.jobs[job_id] = job
            
            logger.info(f"Job {job_id} submitted with {len(input_files)} map tasks and {request.num_reduces} reduce tasks")
            
            return mapreduce_pb2.SubmitJobResponse(
                job_id=job_id,
                success=True,
                message=f"Job submitted with {len(input_files)} map tasks and {request.num_reduces} reduce tasks"
            )

    def GetJobStatus(self, request, context):
        """Get the status of a job."""
        with self.lock:
            if request.job_id not in self.jobs:
                return mapreduce_pb2.GetJobStatusResponse(
                    job_id=request.job_id,
                    status="NOT_FOUND"
                )
            
            job = self.jobs[request.job_id]
            
            map_complete = sum(1 for t in job.map_tasks.values() if t.state == TaskState.COMPLETED)
            reduce_complete = sum(1 for t in job.reduce_tasks.values() if t.state == TaskState.COMPLETED)
            
            return mapreduce_pb2.GetJobStatusResponse(
                job_id=job.job_id,
                status=job.state,
                map_progress=map_complete,
                reduce_progress=reduce_complete,
                total_maps=len(job.map_tasks),
                total_reduces=len(job.reduce_tasks)
            )

    def GetTask(self, request, context):
        """Assign a task to a worker."""
        with self.lock:
            worker_id = request.worker_id
            
            # Try to find a map task first
            task = self._find_available_task("MAP")
            if task:
                return self._assign_task(task, worker_id)
            
            # Check if all map tasks are complete before assigning reduce tasks
            if self._all_map_tasks_complete():
                task = self._find_available_task("REDUCE")
                if task:
                    return self._assign_task(task, worker_id)
            
            # No tasks available
            return mapreduce_pb2.GetTaskResponse(task_type="NONE")

    def ReportTaskComplete(self, request, context):
        """Handle task completion from worker."""
        with self.lock:
            task_id = request.task_id
            
            if task_id not in self.tasks:
                logger.warning(f"Unknown task {task_id} completed")
                return mapreduce_pb2.ReportTaskCompleteResponse(acknowledged=False)
            
            task = self.tasks[task_id]
            
            # Ignore if already completed (e.g., backup finished first)
            if task.state == TaskState.COMPLETED:
                logger.info(f"Task {task_id} already completed, ignoring duplicate")
                return mapreduce_pb2.ReportTaskCompleteResponse(acknowledged=True)
            
            if request.success:
                task.state = TaskState.COMPLETED
                task.end_time = datetime.now()
                logger.info(f"Task {task_id} completed by {request.worker_id}")
                
                # If this has a backup, mark backup as completed too
                if task.backup_task_id and task.backup_task_id in self.tasks:
                    backup = self.tasks[task.backup_task_id]
                    if backup.state != TaskState.COMPLETED:
                        backup.state = TaskState.COMPLETED
                        logger.info(f"Marking backup task {task.backup_task_id} as completed")
                
                # Check if job is complete
                self._check_job_completion(task.job_id)
            else:
                task.state = TaskState.FAILED
                logger.error(f"Task {task_id} failed: {request.error_message}")
                # Could implement retry logic here
            
            return mapreduce_pb2.ReportTaskCompleteResponse(acknowledged=True)

    def Heartbeat(self, request, context):
        """Handle worker heartbeat."""
        with self.lock:
            self.worker_heartbeats[request.worker_id] = datetime.now()
            return mapreduce_pb2.HeartbeatResponse(acknowledged=True)

    def _split_input(self, input_path, num_maps):
        """Split input file into chunks for map tasks."""
        # Read the input file
        full_path = input_path
        if not os.path.exists(full_path):
            logger.error(f"Input file {full_path} not found")
            return []
        
        with open(full_path, 'r') as f:
            lines = f.readlines()
        
        # Calculate lines per chunk
        lines_per_chunk = max(1, len(lines) // num_maps)
        
        # Create chunks
        input_files = []
        intermediate_dir = "/data/intermediate"
        os.makedirs(intermediate_dir, exist_ok=True)
        
        for i in range(num_maps):
            start_idx = i * lines_per_chunk
            if i == num_maps - 1:
                # Last chunk gets all remaining lines
                end_idx = len(lines)
            else:
                end_idx = start_idx + lines_per_chunk
            
            chunk_file = f"{intermediate_dir}/input-chunk-{i}.txt"
            with open(chunk_file, 'w') as f:
                f.writelines(lines[start_idx:end_idx])
            
            input_files.append(chunk_file)
        
        return input_files

    def _find_available_task(self, task_type):
        """Find an idle task of the given type."""
        for task in self.tasks.values():
            if task.task_type == task_type and task.state == TaskState.IDLE and not task.is_backup:
                return task
        return None

    def _assign_task(self, task, worker_id):
        """Assign a task to a worker."""
        task.state = TaskState.IN_PROGRESS
        task.worker_id = worker_id
        task.start_time = datetime.now()
        
        job = self.jobs[task.job_id]
        
        if task.task_type == "MAP":
            return mapreduce_pb2.GetTaskResponse(
                task_id=task.task_id,
                task_type="MAP",
                job_id=task.job_id,
                input_file=task.input_file,
                map_task_number=task.map_task_number,
                num_reduces=job.num_reduces,
                mapper_code=job.mapper_code
            )
        else:  # REDUCE
            return mapreduce_pb2.GetTaskResponse(
                task_id=task.task_id,
                task_type="REDUCE",
                job_id=task.job_id,
                reduce_task_number=task.reduce_task_number,
                num_maps=job.num_maps,
                reducer_code=job.reducer_code
            )

    def _all_map_tasks_complete(self):
        """Check if all map tasks across all jobs are complete."""
        for job in self.jobs.values():
            if job.state == JobState.RUNNING:
                for task in job.map_tasks.values():
                    if task.state != TaskState.COMPLETED:
                        return False
        return True

    def _check_job_completion(self, job_id):
        """Check if a job is complete."""
        job = self.jobs[job_id]
        
        all_maps_done = all(t.state == TaskState.COMPLETED for t in job.map_tasks.values())
        all_reduces_done = all(t.state == TaskState.COMPLETED for t in job.reduce_tasks.values())
        
        if all_maps_done and all_reduces_done:
            job.state = JobState.COMPLETED
            logger.info(f"Job {job_id} completed")

    def _monitor_stragglers(self):
        """Background thread to detect and handle stragglers."""
        while True:
            time.sleep(5)  # Check every 5 seconds
            
            with self.lock:
                for job in self.jobs.values():
                    if job.state != JobState.RUNNING:
                        continue
                    
                    # Check map tasks
                    self._detect_stragglers_for_phase(job, list(job.map_tasks.values()))
                    
                    # Check reduce tasks
                    self._detect_stragglers_for_phase(job, list(job.reduce_tasks.values()))

    def _detect_stragglers_for_phase(self, job, tasks):
        """Detect stragglers in a phase (map or reduce)."""
        # Get completed task durations
        completed_durations = []
        for task in tasks:
            if task.state == TaskState.COMPLETED and task.start_time and task.end_time and not task.is_backup:
                duration = (task.end_time - task.start_time).total_seconds()
                completed_durations.append(duration)
        
        # Need enough completed tasks to establish baseline
        min_tasks_needed = int(len(tasks) * self.min_tasks_for_baseline)
        if len(completed_durations) < max(1, min_tasks_needed):
            return
        
        # Calculate median duration
        completed_durations.sort()
        median_duration = completed_durations[len(completed_durations) // 2]
        
        # Check in-progress tasks for stragglers
        now = datetime.now()
        for task in tasks:
            if task.state == TaskState.IN_PROGRESS and task.start_time and not task.is_backup:
                elapsed = (now - task.start_time).total_seconds()
                
                # Launch backup if taking too long and no backup exists
                if elapsed > median_duration * self.straggler_threshold and not task.backup_task_id:
                    self._launch_backup_task(task, job)

    def _launch_backup_task(self, original_task, job):
        """Launch a backup task for a straggler."""
        backup_task_id = f"{original_task.task_id}-backup"
        
        # Create backup task
        backup = Task(backup_task_id, original_task.task_type, original_task.job_id)
        backup.is_backup = True
        backup.state = TaskState.IDLE
        
        # Copy task-specific info
        if original_task.task_type == "MAP":
            backup.input_file = original_task.input_file
            backup.map_task_number = original_task.map_task_number
        else:
            backup.reduce_task_number = original_task.reduce_task_number
        
        # Link tasks
        original_task.backup_task_id = backup_task_id
        
        self.tasks[backup_task_id] = backup
        
        logger.warning(f"Straggler detected! Launching backup task {backup_task_id} for {original_task.task_id}")

    def _monitor_workers(self):
        """Background thread to monitor worker health."""
        while True:
            time.sleep(10)
            
            with self.lock:
                now = datetime.now()
                timeout = timedelta(seconds=30)
                
                for worker_id, last_heartbeat in list(self.worker_heartbeats.items()):
                    if now - last_heartbeat > timeout:
                        logger.warning(f"Worker {worker_id} appears to be dead")
                        # Could implement task reassignment here


def serve():
    """Start the coordinator server."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapreduce_pb2_grpc.add_CoordinatorServicer_to_server(
        CoordinatorServicer(), server
    )
    
    port = os.getenv('COORDINATOR_PORT', '50051')
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    
    logger.info(f"Coordinator started on port {port}")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    serve()
