import logging
from prometheus_client import start_http_server, Counter, Gauge, Histogram, REGISTRY
import time
from typing import Optional, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('LoggingMetrics')

# Prometheus Metrics
METRICS = {
    "requests_total": Counter(
        'app_requests_total',
        'Total number of requests',
        ['component']
    ),
    "errors_total": Counter(
        'app_errors_total',
        'Total number of errors',
        ['component']
    ),
    "processing_time": Histogram(
        'app_processing_time_seconds',
        'Time spent processing requests',
        ['component']
    ),
    "active_connections": Gauge(
        'app_active_connections',
        'Number of active connections',
        ['component']
    )
}

class LoggingMetrics:
    def __init__(self, metrics_port: int = 8000):
        """
        Initialize the logging and metrics module.
        Args:
            metrics_port (int): Port to expose Prometheus metrics on.
        """
        self.metrics_port = metrics_port
        self._start_metrics_server()

    def _start_metrics_server(self):
        """Start the Prometheus metrics server."""
        start_http_server(self.metrics_port)
        logger.info(f"Prometheus metrics server started on port {self.metrics_port}")

    def log(self, component: str, level: str, message: str, extra: Optional[Dict[str, Any]] = None):
        """
        Log a message with a specific level.
        Args:
            component (str): Name of the component generating the log.
            level (str): Log level (e.g., INFO, WARNING, ERROR).
            message (str): Log message.
            extra (Optional[Dict[str, Any]]): Additional context for the log.
        """
        log_message = f"[{component}] {message}"
        if extra:
            log_message += f" | Extra: {extra}"

        if level.upper() == "INFO":
            logger.info(log_message)
        elif level.upper() == "WARNING":
            logger.warning(log_message)
        elif level.upper() == "ERROR":
            logger.error(log_message)
            self.increment_metric("errors_total", component)
        else:
            logger.debug(log_message)

    def increment_metric(self, metric_name: str, component: str, value: float = 1):
        """
        Increment a Prometheus metric.
        Args:
            metric_name (str): Name of the metric (e.g., "requests_total").
            component (str): Name of the component updating the metric.
            value (float): Value to increment by (default is 1).
        """
        if metric_name in METRICS:
            METRICS[metric_name].labels(component=component).inc(value)
        else:
            logger.warning(f"Metric {metric_name} not found.")

    def set_metric(self, metric_name: str, component: str, value: float):
        """
        Set the value of a Prometheus gauge metric.
        Args:
            metric_name (str): Name of the metric (e.g., "active_connections").
            component (str): Name of the component updating the metric.
            value (float): Value to set.
        """
        if metric_name in METRICS and isinstance(METRICS[metric_name], Gauge):
            METRICS[metric_name].labels(component=component).set(value)
        else:
            logger.warning(f"Metric {metric_name} is not a gauge or does not exist.")

    def observe_metric(self, metric_name: str, component: str, value: float):
        """
        Observe a value for a Prometheus histogram metric.
        Args:
            metric_name (str): Name of the metric (e.g., "processing_time").
            component (str): Name of the component updating the metric.
            value (float): Value to observe.
        """
        if metric_name in METRICS and isinstance(METRICS[metric_name], Histogram):
            METRICS[metric_name].labels(component=component).observe(value)
        else:
            logger.warning(f"Metric {metric_name} is not a histogram or does not exist.")

# Singleton instance for easy access
logging_metrics = LoggingMetrics()