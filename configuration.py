"""
Definition of configurations for components
"""

# Communication Protocol
# Maximum duration to wait for a message to be sent successfully
RETRY_DURATION_IN_SECONDS = 300
# Duration to wait before retrying to send a failed request in seconds
SECONDS_BETWEEN_RETRIES = 15

# Logging
# Severity level (choose from 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
LOGGING_LEVEL = "DEBUG"
# Padding size for log messages
LOGGING_PADDING = 65

# Client Settings
# Maximum interval between sensor readings in seconds
MAX_SENSOR_INTERVAL_IN_SECONDS = 2
