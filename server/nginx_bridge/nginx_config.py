import logging
import os
import subprocess
import time
from . import config

logger = logging.getLogger(__name__)

def update_nginx_upstream(leader_address):
    """
    Updates the NGINX upstream configuration file with the new leader address
    and reloads NGINX.

    Args:
        leader_address (str): The IP:PORT of the current leader.

    Returns:
        bool: True if successful, False otherwise.
    """
    # logger.info(f"Updating NGINX upstream with leader: {leader_address}") # Moved lower

    try:
        # Ensure the NGINX conf.d directory exists
        conf_d_dir = os.path.dirname(config.NGINX_UPSTREAM_CONF)
        if not os.path.exists(conf_d_dir):
            logger.info(f"Creating NGINX conf.d directory: {conf_d_dir}")
            os.makedirs(conf_d_dir, exist_ok=True)

        if not leader_address:
            logger.info("Leader address is empty. Updating NGINX to reflect no available leader.")
            # Create an upstream block with no active servers, or a commented out server
            # Assuming UPSTREAM_TEMPLATE is like: "upstream chat_backend {{\n    server {leader_address};\n}}"
            # We will generate: "upstream chat_backend {{\n    # server 127.0.0.1:1; # No leader available\n}}"
            # This requires UPSTREAM_TEMPLATE to be flexible or we construct it here.
            # When no leader is available, use a dummy server marked as 'down'.
            # This satisfies NGINX's requirement for a server directive in the upstream block.
            server_line = "server 0.0.0.1:1; # Dummy blackhole server to prevent 'no servers are inside upstream' error"
            upstream_content = config.UPSTREAM_TEMPLATE.format(server_line=server_line)
        else:
            logger.info(f"Updating NGINX upstream with leader: {leader_address}")
            # Strip http:// or https:// from the leader_address for the NGINX server directive
            formatted_leader_address = leader_address.replace('http://', '').replace('https://', '')
            server_line = f"server {formatted_leader_address};"
            # Render the upstream configuration
            upstream_content = config.UPSTREAM_TEMPLATE.format(server_line=server_line)


        # Read current configuration to check if an update is actually needed
        current_upstream_content = ""
        if os.path.exists(config.NGINX_UPSTREAM_CONF):
            try:
                with open(config.NGINX_UPSTREAM_CONF, 'r') as f:
                    current_upstream_content = f.read()
            except IOError as e:
                logger.warning(f"Could not read current NGINX upstream config: {e}. Proceeding with update.")

        if upstream_content == current_upstream_content:
            logger.info("NGINX upstream configuration is already up to date. No reload needed.")
            return True

        # Write the new configuration
        with open(config.NGINX_UPSTREAM_CONF, 'w') as f:
            f.write(upstream_content)
        logger.info(f"NGINX upstream configuration written to {config.NGINX_UPSTREAM_CONF}")

        # Reload NGINX
        return reload_nginx()

    except IOError as e:
        logger.error(f"IOError updating NGINX configuration: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error updating NGINX configuration: {e}")
        return False

def reload_nginx():
    """
    Reloads the NGINX configuration.

    Returns:
        bool: True if successful, False otherwise.
    """
    logger.info(f"Attempting to reload NGINX using command: '{config.NGINX_RELOAD_CMD}'")
    try:
        # On Windows, 'nginx -s reload' might not work directly if nginx is not in PATH
        # or if it's running as a service. For development, we might need a different approach.
        # For now, assume 'nginx' command is available and works.
        # In a production Linux environment, this command is standard.
        # Consider using absolute path to nginx if necessary.
        # Construct the reload command with the -p prefix to specify the NGINX installation directory
        # and -c to specify the configuration file, ensuring NGINX knows where to find its files.
        # NGINX_CONF_DIR should point to the directory containing nginx.conf (e.g., C:/nginx-1.27.5)
        # NGINX_RELOAD_CMD should be just 'nginx.exe -s reload'
        # The actual config file NGINX uses is NGINX_CONF_DIR + /conf/nginx.conf
        nginx_executable_path = config.NGINX_RELOAD_CMD.split(' ')[0] #e.g. C:/nginx-1.27.5/nginx.exe
        nginx_install_dir = os.path.dirname(nginx_executable_path) #e.g. C:/nginx-1.27.5
        nginx_conf_file = os.path.join(config.NGINX_CONF_DIR, 'nginx.conf') # e.g. C:/nginx-1.27.5/conf/nginx.conf

        # Test NGINX configuration before reloading
        test_cmd_parts = [
            nginx_executable_path,
            '-p', nginx_install_dir,
            '-c', nginx_conf_file,
            '-t'
        ]
        logger.info(f"Testing NGINX configuration with command: {' '.join(test_cmd_parts)}")
        test_process = subprocess.run(test_cmd_parts, shell=False, check=False, capture_output=True, text=True, cwd=nginx_install_dir)
        if test_process.returncode != 0:
            logger.error(f"NGINX configuration test failed. STDOUT: {test_process.stdout.strip()}, STDERR: {test_process.stderr.strip()}")
            return False
        logger.info(f"NGINX configuration test successful. Output: {test_process.stdout.strip()}")

        reload_cmd_parts = [
            nginx_executable_path,
            '-p', nginx_install_dir, # Set prefix path
            '-c', nginx_conf_file,    # Set configuration file
            '-s', 'reload'
        ]
        logger.info(f"Constructed NGINX reload command: {' '.join(reload_cmd_parts)}")
        process = subprocess.run(reload_cmd_parts, shell=False, check=True, capture_output=True, text=True, cwd=nginx_install_dir)
        logger.info(f"NGINX reload successful. Output: {process.stdout.strip()}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to reload NGINX. Return code: {e.returncode}")
        logger.error(f"NGINX reload STDOUT: {e.stdout.strip()}")
        logger.error(f"NGINX reload STDERR: {e.stderr.strip()}")
        return False
    except FileNotFoundError:
        logger.error(f"NGINX command not found. Ensure NGINX is installed and in PATH or NGINX_RELOAD_CMD is correct.")
        return False
    except Exception as e:
        logger.error(f"Unexpected error reloading NGINX: {e}")
        return False

if __name__ == '__main__':
    # Basic test for the module
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Create dummy NGINX conf dir for local testing if it doesn't exist
    # In a real scenario, NGINX_CONF_DIR would point to the actual NGINX config path
    if not os.path.exists(config.NGINX_CONF_DIR):
        try:
            os.makedirs(config.NGINX_CONF_DIR, exist_ok=True)
            logger.info(f"Created dummy NGINX config directory for testing: {config.NGINX_CONF_DIR}")
        except Exception as e:
            logger.error(f"Could not create dummy NGINX config directory: {e}")

    # Test updating with a dummy leader address
    test_leader = "127.0.0.1:8080"
    logger.info(f"Testing NGINX update with leader: {test_leader}")
    if update_nginx_upstream(test_leader):
        logger.info("NGINX update test successful.")
    else:
        logger.error("NGINX update test failed.")
        logger.warning("Please ensure NGINX is installed and configured correctly for the reload command to work.")
        logger.warning(f"Current NGINX_CONF_DIR: {config.NGINX_CONF_DIR}")
        logger.warning(f"Current NGINX_UPSTREAM_CONF: {config.NGINX_UPSTREAM_CONF}")
        logger.warning(f"Current NGINX_RELOAD_CMD: {config.NGINX_RELOAD_CMD}")