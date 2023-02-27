import threading
import queue

# Define the number of worker threads for the map and reduce tasks
NUM_MAP_THREADS = 4
NUM_REDUCE_THREADS = 2

# Define the data storage directory for each worker
DATA_DIR = '/tmp/mr'

# Define the interface for the master node
class MasterNode:
    def __init__(self):
        self.map_tasks = queue.Queue()
        self.reduce_tasks = queue.Queue()
        self.map_results = {}
        self.reduce_results = {}
        self.done = threading.Event()

    def init_cluster(self, nodes):
        pass

    def run_mapred(self, input_data, map_fn, reduce_fn, output_location):
        # Split the input data into chunks and create map tasks
        input_chunks = split_input_data(input_data, NUM_MAP_THREADS)
        for chunk in input_chunks:
            self.map_tasks.put(chunk)

        # Create map worker threads
        map_threads = []
        for i in range(NUM_MAP_THREADS):
            t = threading.Thread(target=map_worker, args=(map_fn,))
            map_threads.append(t)
            t.start()

        # Wait for all map tasks to complete
        self.map_tasks.join()

        # Create reduce tasks from the map results
        for key, values in self.map_results.items():
            self.reduce_tasks.put((key, values))

        # Create reduce worker threads
        reduce_threads = []
        for i in range(NUM_REDUCE_THREADS):
            t = threading.Thread(target=reduce_worker, args=(reduce_fn,))
            reduce_threads.append(t)
            t.start()

        # Wait for all reduce tasks to complete
        self.reduce_tasks.join()

        # Write the output data to the output location
        write_output_data(output_location, self.reduce_results)

    def destroy_cluster(self, cluster_id):
        self.done.set()

# Define the map worker function
def map_worker(map_fn):
    while not master.done.is_set():
        try:
            chunk = master.map_tasks.get(timeout=1)
            results = {}
            for key, value in map_fn(chunk):
                if key not in results:
                    results[key] = []
                results[key].append(value)
            master.map_results.update(results)
        finally:
            master.map_tasks.task_done()

# Define the reduce worker function
def reduce_worker(reduce_fn):
    while not master.done.is_set():
        try:
            key, values = master.reduce_tasks.get(timeout=1)
            result = reduce_fn(key, values)
            master.reduce_results[key] = result
        finally:
            master.reduce_tasks.task_done()

# Helper functions
def split_input_data(input_data, num_chunks):
    chunk_size = len(input_data) // num_chunks
    input_chunks = [input_data[i:i+chunk_size] for i in range(0, len(input_data), chunk_size)]
    return input_chunks

def write_output_data(output_location, output_data):
    with open(output_location, 'w') as f:
        for key, value in output_data.items():
            f.write('{}\t{}\n'.format(key, value))

# Word count map and reduce functions
def word_count_map(chunk):
    for word in chunk.split():
        yield (word.lower(), 1)

def word_count_reduce(key, values):
    return sum(values)

# Inverted index map and reduce functions
def inverted_index_map(chunk):
    for i, word in enumerate(chunk.split()):
        yield (word.lower(), i)

def inverted_index_reduce(key, values):
    return sorted(values)

