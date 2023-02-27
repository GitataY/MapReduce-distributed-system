import threading
import queue
import uuid
import os
import shutil
import logging

from mapreduce import MapReduce
from worker import WorkerNode
from utils import split_input_data, write_output_data


class MasterNode:
    def __init__(self):
        self.map_tasks = queue.Queue()
        self.reduce_tasks = queue.Queue()
        self.map_results = {}
        self.reduce_results = {}
        self.done = threading.Event()
        self.cluster_id = str(uuid.uuid4())
        self.workers = []

        # Set up logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        # Create console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # Add formatter to console handler
        ch.setFormatter(formatter)

        # Add console handler to logger
        self.logger.addHandler(ch)

    def init_cluster(self, nodes):
        for node in nodes:
            self.logger.debug(f"Connecting to worker node {node}...")
            worker = WorkerNode(node, self.cluster_id)
            worker.connect()
            self.workers.append(worker)

    def run_mapred(self, input_data, map_fn, reduce_fn, output_location):
        # Split the input data into chunks and create map tasks
        input_chunks = split_input_data(input_data, len(self.workers))
        for i, chunk in enumerate(input_chunks):
            self.map_tasks.put((i, chunk))

        # Create map tasks on worker nodes
        for i, worker in enumerate(self.workers):
            task_id, task_data = self.map_tasks.get()
            worker.run_map_task(map_fn, task_id, task_data)

        # Wait for all map tasks to complete
        for worker in self.workers:
            worker.wait_for_map_tasks()

        # Create reduce tasks from the map results
        for key, values in self.map_results.items():
            self.reduce_tasks.put((key, values))

        # Create reduce tasks on worker nodes
        for i, worker in enumerate(self.workers):
            task_id, task_data = self.reduce_tasks.get()
            worker.run_reduce_task(reduce_fn, task_id, task_data)

        # Wait for all reduce tasks to complete
        for worker in self.workers:
            worker.wait_for_reduce_tasks()

        # Write the output data to the output location
        write_output_data(output_location, self.reduce_results)

        # Clean up data directories on worker nodes
        self.cleanup()

    def cleanup(self):
        for worker in self.workers:
            data_dir = os.path.join(worker.data_dir, self.cluster_id)
            shutil.rmtree(data_dir)
            self.logger.debug(f"Cleaned up data directory {data_dir}")

    def destroy_cluster(self):
        self.done.set()
        for worker in self.workers:
            worker.disconnect()
