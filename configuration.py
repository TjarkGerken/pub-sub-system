"""
Definition of changeable configurations

Configuration options:
    - Communication Protocol: Settings related to message sending and retry mechanisms
    - Logging: Settings for logging severity levels and message formatting.
    - Client Settings: Settings related to sensors (publishers) and subscribers
"""

# Communication Protocol
## Maximum duration to wait for a message to be sent successfully
RETRY_DURATION_IN_SECONDS = 300
## Duration to wait before retrying to send a failed request in secnods
SECONDS_BETWEEN_RETRIES = 45

# Logging
## Severity level (choose from 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
LOGGING_LEVEL = "INFO"
## Padding size for log messages
LOGGING_PADDING = 65

# Client Settings
## Maximum interval between sensor readings in seconds
MAX_SENSOR_INTERVAL_IN_SECONDS = 1