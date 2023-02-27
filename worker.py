import os
import sys
import time
import socket
import importlib
import multiprocessing as mp

def map_task(data, map_fn):
    results = []
    for key, value in map_fn(data):
        results.append((key, value))
    return results

def reduce_task(key, values, reduce_fn):
    return reduce_fn(key, values)

def run_map_task(task_id, task_data, map_fn, output_dir):
    try:
        # Run the map task
        results = map_task(task_data, map_fn)

        # Write the output to a file
        output_file = os.path.join(output_dir, 'map_output_{}.txt'.format(task_id))
        with open(output_file, 'w') as f:
            for key, value in results:
                f.write('{}\t{}\n'.format(key, value))

        # Return the output file path
        return output_file
    except Exception as e:
        # If there was an error, return None
        print('Error running map task: {}'.format(e))
        return None

def run_reduce_task(task_id, task_data, reduce_fn, output_dir):
    try:
        # Unpack the task data
        key, values = task_data

        # Run the reduce task
        result = reduce_task(key, values, reduce_fn)

        # Write the output to a file
        output_file = os.path.join(output_dir, 'reduce_output_{}.txt'.format(task_id))
        with open(output_file, 'w') as f:
            f.write('{}\t{}\n'.format(result[0], result[1]))

        # Return the output file path
        return output_file
    except Exception as e:
        # If there was an error, return None
        print('Error running reduce task: {}'.format(e))
        return None

if __name__ == '__main__':
    # Parse command-line arguments
    task_type = sys.argv[1]
    task_id = sys.argv[2]
    input_file = sys.argv[3]
    output_dir = sys.argv[4]
    module_name = sys.argv[5]
    function_name = sys.argv[6]

    # Import the map/reduce function from the module
    module = importlib.import_module(module_name)
    function = getattr(module, function_name)

    # Read the input data
    with open(input_file, 'r') as f:
        data = f.read()

    # Run the appropriate task
    if task_type == 'map':
        output_file = run_map_task(task_id, data, function, output_dir)
    elif task_type == 'reduce':
        output_file = run_reduce_task(task_id, data, function, output_dir)

    # Print the output file path (or None if there was an error)
    print(output_file)
