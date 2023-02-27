from flask import Flask, jsonify, request
import cluster

app = Flask(__name__)

# Initialize cluster
@app.route('/init_cluster', methods=['POST'])
def init_cluster():
    # Get list of (ip_address, port) tuples from request body
    nodes = request.json.get('nodes')
    if not nodes:
        return jsonify({'error': 'Nodes list not found in request body'}), 400
    # Initialize cluster and return cluster_id
    cluster_id = cluster.initialize_cluster(nodes)
    return jsonify({'cluster_id': cluster_id})

# Run MapReduce job
@app.route('/run_mapred', methods=['POST'])
def run_mapred():
    # Get input data, map function, reduce function, and output location from request body
    input_data = request.json.get('input_data')
    if not input_data:
        return jsonify({'error': 'Input data not found in request body'}), 400
    map_fn = request.json.get('map_fn')
    if not map_fn:
        return jsonify({'error': 'Map function not found in request body'}), 400
    reduce_fn = request.json.get('reduce_fn')
    if not reduce_fn:
        return jsonify({'error': 'Reduce function not found in request body'}), 400
    output_location = request.json.get('output_location')
    if not output_location:
        return jsonify({'error': 'Output location not found in request body'}), 400
    # Run MapReduce job and return result
    try:
        result = cluster.run_mapred(input_data, map_fn, reduce_fn, output_location)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    return jsonify({'result': result})

# Destroy cluster
@app.route('/destroy_cluster', methods=['POST'])
def destroy_cluster():
    # Get cluster_id from request body
    cluster_id = request.json.get('cluster_id')
    if not cluster_id:
        return jsonify({'error': 'Cluster ID not found in request body'}), 400
    # Destroy cluster and return success message
    try:
        cluster.destroy_cluster(cluster_id)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    return jsonify({'message': 'Cluster destroyed successfully'})

if __name__ == '__main__':
    app.run()
