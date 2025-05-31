import json
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading

class RaftRPCServer(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == "/request_vote":
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data)
            response = self.server.raft_node.request_vote(
                data['term'],
                data['candidate_id'],
                data['last_log_index'],
                data['last_log_term']
            )
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response).encode())
        elif self.path == "/append_entries":
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data)
            response = self.server.raft_node.append_entries(
                data['term'],
                data['leader_id'],
                data['prev_log_index'],
                data['prev_log_term'],
                data['entries'],
                data['leader_commit']
            )
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response).encode())
        else:
            self.send_response(404)
            self.end_headers()
    def __init__(self, *args, node=None, **kwargs):
        self.node = node
        super().__init__(*args, **kwargs)
    
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        data = json.loads(self.rfile.read(content_length))
        
        if self.path == '/request_vote':
            response = self.node.request_vote(
                data['term'],
                data['candidate_id'],
                data['last_log_index'],
                data['last_log_term']
            )
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response).encode())
            
        elif self.path == '/append_entries':
            response = self.node.append_entries(
                data['term'],
                data['leader_id'],
                data['prev_log_index'],
                data['prev_log_term'],
                data['entries'],
                data['leader_commit']
            )
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response).encode())

def start_rpc_server(node, port=8000):
    def handler(*args):
        return RaftRPCServer(*args, node=node)
    server = HTTPServer(('', port), handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    return server