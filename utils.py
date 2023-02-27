import os

def split_input_data(input_data, num_chunks):
    """Split input data into num_chunks chunks."""
    input_chunks = []
    chunk_size = len(input_data) // num_chunks
    remainder = len(input_data) % num_chunks
    start = 0
    for i in range(num_chunks):
        if i < remainder:
            end = start + chunk_size + 1
        else:
            end = start + chunk_size
        input_chunks.append(input_data[start:end])
        start = end
    return input_chunks

def write_output_data(output_location, data):
    """Write output data to a specified location."""
    with open(output_location, 'w') as f:
        for key, value in data.items():
            f.write(f"{key}\t{value}\n")

def read_input_data(input_location):
    """Read input data from a specified location."""
    with open(input_location, 'r') as f:
        input_data = f.read()
    return input_data

def create_temp_dir():
    """Create a temporary directory for worker nodes."""
    dir_path = f"/tmp/mr-{os.getpid()}"
    os.makedirs(dir_path)
    return dir_path

def cleanup_temp_dir(dir_path):
    """Clean up temporary directories after the MapReduce job has completed."""
    os.system(f"rm -rf {dir_path}")
