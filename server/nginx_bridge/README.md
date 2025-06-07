# NGINX Bridge Service

This service dynamically updates NGINX upstream configuration based on leader information published in etcd. It ensures that NGINX routes traffic to the currently elected leader of the distributed chat application.

## Prerequisites

1.  **Python 3.7+**
2.  **etcd Server**: Accessible to this service.
3.  **NGINX**: Installed and configured.
4.  **Existing Distributed Chat Application**: The server component that performs leader election and publishes leader info to etcd under the key `/raft/leader`.

## Configuration

The bridge service is configured via environment variables. See `config.py` for details. Key variables include:

-   `ETCD_ENDPOINTS`: Comma-separated list of etcd server endpoints (e.g., `http://localhost:2379,http://etcd2:2379`). Defaults to `http://localhost:2379`.
-   `NGINX_CONF_DIR`: Path to the NGINX configuration directory (e.g., `/etc/nginx` on Linux, or `C:/nginx/conf` on Windows). Defaults to `/etc/nginx`. The upstream configuration will be written to `conf.d/chat_upstream.conf` within this directory.
-   `NGINX_RELOAD_CMD`: Command to reload NGINX (e.g., `nginx -s reload` or `sudo systemctl reload nginx`). Defaults to `nginx -s reload`.
-   `LOG_LEVEL`: Logging level (e.g., `INFO`, `DEBUG`). Defaults to `INFO`.

**Important for Windows NGINX:**
-   Ensure the `NGINX_CONF_DIR` points to your NGINX `conf` directory (e.g., `C:/nginx/conf`).
-   The `NGINX_RELOAD_CMD` might need to be adjusted. `nginx -s reload` works if `nginx.exe` is in your system PATH and you have permissions. You might need to use the full path to `nginx.exe` (e.g., `C:/nginx/nginx.exe -s reload`).

## Installation

1.  Navigate to the `DistributedChatApplication/server/nginx_bridge` directory.
2.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

## Running the Service

From the `DistributedChatApplication/server` directory, you can run the service as a module:

```bash
python -m nginx_bridge.bridge_service
```

Alternatively, navigate to `DistributedChatApplication/server/nginx_bridge` and run:

```bash
python bridge_service.py
```

Ensure your environment variables are set correctly before running.

## NGINX Setup

1.  The bridge service will create/update a file named `chat_upstream.conf` inside the `conf.d` subdirectory of your `NGINX_CONF_DIR`. For example, if `NGINX_CONF_DIR` is `/etc/nginx`, the file will be `/etc/nginx/conf.d/chat_upstream.conf`.

2.  Ensure your main NGINX configuration file (e.g., `nginx.conf`) includes configurations from the `conf.d` directory. This is usually done with a line like:
    ```nginx
    include /etc/nginx/conf.d/*.conf;
    ```
    Adjust the path according to your `NGINX_CONF_DIR`.

3.  Your NGINX server block that proxies to the chat application should use the `chat_backend` upstream defined in `chat_upstream.conf`:
    ```nginx
    server {
        listen 80;
        server_name your_chat_app_domain.com;

        location / {
            proxy_pass http://chat_backend;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            proxy_http_version 1.1;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection "upgrade";
        }
    }
    ```

## Running as a Service (Production)

-   **Linux**: You would typically use a `systemd` service file. (An example can be provided if targeting Linux).
-   **Windows**: To run the `bridge_service.py` script as a Windows service, you can use tools like:
    -   **NSSM (Non-Sucking Service Manager)**: A popular choice for easily creating Windows services from scripts. You would configure NSSM to run the `python.exe` interpreter with `bridge_service.py` as an argument.
    -   **Windows Task Scheduler**: Configure a task to run at system startup with appropriate permissions.

Ensure the service has permissions to write to the NGINX configuration directory and execute the NGINX reload command.

## Development Notes

-   The `nginx_config.py` module contains a basic test execution block (`if __name__ == '__main__':`) that can be used for manually testing NGINX update functionality. It will attempt to create a dummy NGINX config directory if `NGINX_CONF_DIR` doesn't exist, for local testing without a full NGINX setup (though reload will likely fail).