"""NGINX Bridge Service for Dynamic Leader Routing

This package provides a bridge service that watches etcd for leader changes
and dynamically updates NGINX upstream configuration to route traffic to the
current leader node.
"""

from . import config
from . import nginx_config
from .bridge_service import NginxBridgeService

__all__ = ['config', 'nginx_config', 'NginxBridgeService']