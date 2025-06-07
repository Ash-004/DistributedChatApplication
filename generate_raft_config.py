import yaml
import argparse

def generate_raft_config(num_nodes):
    config = {
        'cluster': {
            'nodes': []
        },
        'etcd_config': {
            'ttl': 30,
            'heartbeat_interval': 10,
            'leader_ttl': 30
        },
        'raft_config': {
            'election_timeout_min_ms': 1500,
            'election_timeout_max_ms': 3000,
            'heartbeat_interval_ms': 500
        }
    }

    base_api_port = 8001
    base_rpc_port = 5001

    for i in range(1, num_nodes + 1):
        node_id = f'node{i}'
        api_port = base_api_port + (i - 1)
        rpc_port = base_rpc_port + (i - 1)
        
        node_config = {
            'node_id': node_id,
            'api_host': '127.0.0.1',
            'api_port': api_port,
            'rpc_host': '127.0.0.1',
            'rpc_port': rpc_port
        }
        config['cluster']['nodes'].append(node_config)

    with open('raft_config.yaml', 'w') as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    print(f"Generated raft_config.yaml with {num_nodes} nodes.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate raft_config.yaml for a specified number of nodes.')
    parser.add_argument('num_nodes', type=int, help='The number of nodes to generate.')
    args = parser.parse_args()
    generate_raft_config(args.num_nodes)