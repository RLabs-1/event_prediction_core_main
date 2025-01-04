import requests
import time
import subprocess
from logging_metrics import logging_metrics  # Import your logging and metrics module

# LFR instance details
lfr_instances = [
    {"name": "LFR-1", "url": "http://lfr1.example.com/health"},
    {"name": "LFR-2", "url": "http://lfr2.example.com/health"}
]

# check_health function: This function checks the health of an LFR instance by sending a 
# GET request to its health endpoint (lfr_instance["url"]).
def check_health(lfr_instance):
    try:
        # Send a GET request to the health endpoint
        response = requests.get(lfr_instance["url"])
        if response.status_code == 200:
            logging_metrics.log("LFRHealthCheck", "INFO", f"{lfr_instance['name']} is healthy.")
            return True
        else:
            logging_metrics.log("LFRHealthCheck", "WARNING", f"{lfr_instance['name']} is unhealthy. Status code: {response.status_code}")
            return False
    except Exception as e:
        logging_metrics.log("LFRHealthCheck", "ERROR", f"Error checking {lfr_instance['name']} health: {e}")
        return False

# retry_restart function:: This function is responsible for attempting to restart a list of
# unhealthy LFR instances up to a maximum number of times (max_retries)
def retry_restart(unhealthy_instances, max_retries=3):
    for lfr_instance in unhealthy_instances:
        for attempt in range(max_retries):
            logging_metrics.log("LFRRetry", "INFO", f"Attempting to restart {lfr_instance['name']} (Attempt {attempt + 1})")
            # Restart logic for Docker
            success = restart_instance(lfr_instance["name"])  
            if success:
                logging_metrics.log("LFRRetry", "INFO", f"{lfr_instance['name']} restarted successfully.")
                break
             # Wait before retrying
            time.sleep(5) 
        else:
            logging_metrics.log("LFRRetry", "ERROR", f"Failed to restart {lfr_instance['name']} after {max_retries} attempts.")

# restart_instance function: This function restarts the Docker container of the
# LFR instance using the subprocess.run method.
def restart_instance(instance_name):
    try:
        # Restart the Docker container using the container's name
        logging_metrics.log("LFRRestart", "INFO", f"Attempting to restart Docker container: {instance_name}")
        # Restart the container
        subprocess.run(["docker", "restart", instance_name], check=True)  
        logging_metrics.log("LFRRestart", "INFO", f"Successfully restarted Docker container: {instance_name}")
        return True
    except subprocess.CalledProcessError as e:
        logging_metrics.log("LFRRestart", "ERROR", f"Failed to restart Docker container {instance_name}: {e}")
        return False

# monitor_lfr_instances function: This function continuously monitors the health of the
# LFR instances.
def monitor_lfr_instances():
    # List to track unhealthy LFR instances
    unhealthy_instances = []  
    while True:
        for lfr in lfr_instances:
            healthy = check_health(lfr)
            if not healthy:
                # Add unhealthy instance to the list
                unhealthy_instances.append(lfr)

        # Retry restarting for all unhealthy instances
        if unhealthy_instances:
            retry_restart(unhealthy_instances)
            # Clear the list after attempting restarts
            unhealthy_instances.clear()  
         # Check health every 60 seconds
        time.sleep(60) 

# Start monitoring
if __name__ == "__main__":
    monitor_lfr_instances()