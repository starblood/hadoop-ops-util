#!/bin/bash

# Directory where logs are stored (please change to the actual path)
LOG_DIR="<log-directory>"

# Move to the log directory
cd "$LOG_DIR" || exit 1

# 1. ambari-agent.log.N - Delete logs older than 2 weeks
find . -type f -name "ambari-agent.log.*" -mtime +14 -exec echo "deleting...: {}" \; -exec rm {} \;


# 2. ambari-alerts.log.N - Delete logs older than 2 weeks
find . -type f -name "ambari-alerts.log.*" -mtime +14 -exec echo "deleting...: {}" \; -exec rm {} \;
