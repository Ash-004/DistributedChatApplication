import os

# Etcd Configuration
ETCD_ENDPOINTS = os.getenv('ETCD_ENDPOINTS', 'http://localhost:2379').split(',')
ETCD_LEADER_KEY = '/raft/leader'
ETCD_WATCH_TIMEOUT = 30  # seconds

# NGINX Configuration
NGINX_CONF_DIR = 'C:/nginx-1.27.5/conf'
NGINX_UPSTREAM_CONF = os.path.join(NGINX_CONF_DIR, 'conf.d/chat_upstream.conf')
NGINX_RELOAD_CMD = 'C:/nginx-1.27.5/nginx.exe -s reload'

# Service Configuration
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
UPDATE_RETRY_INTERVAL = 5  # seconds between retries on failure
MAX_RETRIES = 3  # maximum number of retries for operations

# Health Check Configuration
HEALTH_CHECK_INTERVAL = 10  # seconds between health checks
HEALTH_CHECK_TIMEOUT = 5  # seconds before health check times out

# Template Configuration
UPSTREAM_TEMPLATE = """
upstream chat_backend {{
    {server_line}
    keepalive 32;
}}
"""
