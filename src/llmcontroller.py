from kubernetes import client, config
from typing import Dict, List, Tuple
import yaml
import time
import requests
from kubernetes.client.rest import ApiException
from prometheus_client import CollectorRegistry, Counter, Gauge, start_http_server
from logging_metrics import logging_metrics  # Import your logging and metrics module

class LMMController:
    def __init__(self, config_path: str):
        """
        Initialize the LMM Controller
        Args:
            config_path (str): Path to YAML configuration file
        """
        # Initialize Kubernetes client
        try:
            config.load_kube_config()
            self.k8s_apps_v1 = client.AppsV1Api()
            self.k8s_core_v1 = client.CoreV1Api()
            self.k8s_custom = client.CustomObjectsApi()
        except config.ConfigException as e:
            logging_metrics.log("LMMController", "ERROR", f"Failed to load Kubernetes config: {e}")
            raise RuntimeError(f"Failed to load Kubernetes config: {e}")

        # Load LMM configuration
        self.config_path = config_path
        self.config = self._load_config(config_path)
        self.namespace = self.config['kubernetes']['namespace']

        # Initialize metrics
        self._setup_metrics()

    def _setup_metrics(self):
        """Initialize Prometheus metrics"""
        self.registry = CollectorRegistry()
        
        # Create metrics
        self.deployment_count = Gauge(
            'lmm_lfr_deployments_total',
            'Number of LFR deployments',
            registry=self.registry
        )
        self.cpu_usage = Gauge(
            'lmm_lfr_cpu_usage_percent',
            'CPU usage percentage per LFR deployment',
            ['source'],
            registry=self.registry
        )
        self.memory_usage = Gauge(
            'lmm_lfr_memory_usage_bytes',
            'Memory usage in bytes per LFR deployment',
            ['source'],
            registry=self.registry
        )
        self.health_status = Gauge(
            'lmm_lfr_health_status',
            'Health status of LFR deployments (1=healthy, 0=unhealthy)',
            ['source'],
            registry=self.registry
        )
        
        # Start metrics server
        start_http_server(8000, registry=self.registry)

    def _load_config(self, config_path: str) -> Dict:
        """
        Load and validate YAML configuration file
        Args:
            config_path (str): Path to configuration file
        Returns:
            Dict: Parsed configuration
        """
        try:
            with open(config_path) as f:
                config = yaml.safe_load(f)
            self._validate_config(config)
            return config
        except yaml.YAMLError as e:
            logging_metrics.log("LMMController", "ERROR", f"Invalid YAML configuration: {e}")
            raise ValueError(f"Invalid YAML configuration: {e}")
        except FileNotFoundError:
            logging_metrics.log("LMMController", "ERROR", f"Configuration file not found: {config_path}")
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

    def _validate_config(self, config: Dict) -> None:
        """
        Validate configuration structure and values
        Args:
            config (Dict): Configuration to validate
        Raises:
            ValueError: If configuration is invalid
        """
        required_fields = ['kubernetes', 'log_sources']
        for field in required_fields:
            if field not in config:
                logging_metrics.log("LMMController", "ERROR", f"Missing required field: {field}")
                raise ValueError(f"Missing required field: {field}")

    def create_lfr_deployment(self, source_name: str, replicas: int) -> None:
        """Create a new LFR deployment for a log source"""
        try:
            # Prepare deployment configuration
            deployment = {
                'apiVersion': 'apps/v1',
                'kind': 'Deployment',
                'metadata': {
                    'name': f'lfr-{source_name}',
                    'namespace': self.namespace
                },
                'spec': {
                    'replicas': replicas,
                    'selector': {
                        'matchLabels': {
                            'app': 'lfr',
                            'source': source_name
                        }
                    },
                    'template': {
                        'metadata': {
                            'labels': {
                                'app': 'lfr',
                                'source': source_name
                            }
                        },
                        'spec': {
                            'containers': [{
                                'name': 'lfr',
                                'image': 'lmm-lfr:latest',
                                'ports': [{
                                    'containerPort': 8080,
                                    'name': 'health'
                                }],
                                'env': [
                                    {
                                        'name': 'LOG_SOURCE',
                                        'value': source_name
                                    }
                                ]
                            }]
                        }
                    }
                }
            }
            
            # Create the deployment
            self.k8s_apps_v1.create_namespaced_deployment(
                body=deployment,
                namespace=self.namespace
            )
            logging_metrics.log("LMMController", "INFO", f"Created LFR deployment for source {source_name}")
            
        except client.rest.ApiException as e:
            logging_metrics.log("LMMController", "ERROR", f"Failed to create deployment: {e}")
            raise

    def scale_lfr_deployment(self, source_name: str, replicas: int) -> None:
        """Scale an existing LFR deployment"""
        try:
            # Get current deployment
            deployment = self.k8s_apps_v1.read_namespaced_deployment(
                name=f'lfr-{source_name}',
                namespace=self.namespace
            )
            
            # Update replica count
            deployment.spec.replicas = replicas
            
            # Apply the update
            self.k8s_apps_v1.patch_namespaced_deployment(
                name=f'lfr-{source_name}',
                namespace=self.namespace,
                body=deployment
            )
            logging_metrics.log("LMMController", "INFO", f"Scaled LFR deployment {source_name} to {replicas} replicas")
            
        except client.rest.ApiException as e:
            logging_metrics.log("LMMController", "ERROR", f"Failed to scale deployment: {e}")
            raise

    def delete_lfr_deployment(self, source_name: str) -> None:
        """Delete an LFR deployment"""
        try:
            self.k8s_apps_v1.delete_namespaced_deployment(
                name=f'lfr-{source_name}',
                namespace=self.namespace
            )
            logging_metrics.log("LMMController", "INFO", f"Deleted LFR deployment for source {source_name}")
            
        except client.rest.ApiException as e:
            logging_metrics.log("LMMController", "ERROR", f"Failed to delete deployment: {e}")
            raise

    def _check_endpoint_health(self, source_name: str) -> bool:
        """Check health endpoint of LFR instance"""
        try:
            # Get pod IP and construct health endpoint URL
            pods = self.k8s_core_v1.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=f'app=lfr,source={source_name}'
            )
            
            for pod in pods.items:
                if pod.status.pod_ip:
                    url = f"http://{pod.status.pod_ip}:8080/health"
                    response = requests.get(url, timeout=5)
                    if response.status_code != 200:
                        return False
            return True
        except Exception as e:
            logging_metrics.log("LMMController", "ERROR", f"Health check failed for {source_name}: {e}")
            return False

    def monitor_lfr_health(self) -> Dict[str, bool]:
        """Monitor health of LFR deployments"""
        health_status = {}
        try:
            deployments = self.k8s_apps_v1.list_namespaced_deployment(
                namespace=self.namespace,
                label_selector='app=lfr'
            )
            
            for deployment in deployments.items:
                source_name = deployment.metadata.labels['source']
                desired_replicas = deployment.spec.replicas
                available_replicas = deployment.status.available_replicas
                
                deployment_healthy = (
                    available_replicas is not None and 
                    available_replicas == desired_replicas
                )
                endpoint_healthy = self._check_endpoint_health(source_name)
                
                health_status[source_name] = deployment_healthy and endpoint_healthy
                
                # Update health metric
                self.health_status.labels(source=source_name).set(
                    1 if health_status[source_name] else 0
                )
            
            return health_status
            
        except client.rest.ApiException as e:
            logging_metrics.log("LMMController", "ERROR", f"Failed to check deployment health: {e}")
            return health_status

    def _handle_unhealthy_deployment(self, source_name: str) -> None:
        """
        Handle unhealthy deployment recovery
        Args:
            source_name (str): Name of the unhealthy deployment
        """
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                retry_count += 1
                logging_metrics.log("LMMController", "INFO", f"Recovery attempt {retry_count}/{max_retries} for {source_name}")
                
                # Get deployment details
                deployment = self.k8s_apps_v1.read_namespaced_deployment(
                    name=f'lfr-{source_name}',
                    namespace=self.namespace
                )

                # Force restart by updating deployment
                patch = {
                    "spec": {
                        "template": {
                            "metadata": {
                                "annotations": {
                                    "kubectl.kubernetes.io/restartedAt": 
                                        time.strftime('%Y-%m-%dT%H:%M:%SZ')
                                }
                            }
                        }
                    }
                }
                
                self.k8s_apps_v1.patch_namespaced_deployment(
                    name=f'lfr-{source_name}',
                    namespace=self.namespace,
                    body=patch
                )
                
                # Wait and check if recovery was successful
                time.sleep(10)
                if self._check_endpoint_health(source_name):
                    logging_metrics.log("LMMController", "INFO", f"Recovery successful for {source_name}")
                    return
                    
            except ApiException as e:
                logging_metrics.log("LMMController", "ERROR", f"Recovery attempt {retry_count} failed: {e}")
                
            time.sleep(5)  # Wait before next retry
        
        logging_metrics.log("LMMController", "ERROR", f"Failed to recover {source_name} after {max_retries} attempts")

    def manage_lfr_instances(self) -> None:
        """Main control loop for managing LFR instances"""
        logging_metrics.log("LMMController", "INFO", "Starting LFR instance management")
        
        while True:
            try:
                # Reload configuration (in case it changed)
                self.config = self._load_config(self.config_path)
                
                # Get current deployments
                current_deployments = self.k8s_apps_v1.list_namespaced_deployment(
                    namespace=self.namespace,
                    label_selector='app=lfr'
                )
                current_sources = {
                    d.metadata.labels['source']: d 
                    for d in current_deployments.items
                }
                
                # Update deployment count metric
                self.deployment_count.set(len(current_sources))

                # Check configured sources
                for source in self.config['log_sources']:
                    source_name = source['name']
                    
                    if source['enabled']:
                        if source_name not in current_sources:
                            # Create new deployment with minimum replicas
                            self.create_lfr_deployment(
                                source_name,
                                source['instances']['min']
                            )
                        else:
                            # Check metrics and scale if needed
                            if self.check_scaling_needs(source_name):
                                current_replicas = self.get_current_replicas(source_name)
                                self.scale_lfr_deployment(
                                    source_name,
                                    current_replicas + 1
                                )
                    else:
                        if source_name in current_sources:
                            self.delete_lfr_deployment(source_name)
                
                # Health check
                health_status = self.monitor_lfr_health()
                for source_name, is_healthy in health_status.items():
                    if not is_healthy:
                        logging_metrics.log("LMMController", "WARNING", f"Unhealthy deployment detected: {source_name}")
                        self._handle_unhealthy_deployment(source_name)

                time.sleep(30)  # Control loop interval
                
            except Exception as e:
                logging_metrics.log("LMMController", "ERROR", f"Error in control loop: {e}")
                time.sleep(60)  # Wait longer on error

# Usage example
if __name__ == "__main__":
    controller = LMMController("/etc/lmm/config/lmm-config.yaml")
    controller.manage_lfr_instances()