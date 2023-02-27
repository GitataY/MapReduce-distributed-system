import mapreduce
import json
import subprocess
import time

# Define map and reduce functions
def word_count_map(doc):
    for word in doc['text'].split():
        yield (word.lower(), 1)

def word_count_reduce(key, values):
    return sum(values)

# Load input data
with open('input.json', 'r') as f:
    input_data = json.load(f)

# Run MapReduce job with fault tolerance
while True:
    try:
        output_data = mapreduce.run_mapreduce(input_data, word_count_map, word_count_reduce)
        break
    except subprocess.CalledProcessError:
        # Process was killed, wait and retry
        time.sleep(10)

# Load expected output
with open('expected_output.json', 'r') as f:
    expected_output = json.load(f)

# Check output against expected output
assert output_data == expected_output
print('Word count fault tolerance test passed!')
