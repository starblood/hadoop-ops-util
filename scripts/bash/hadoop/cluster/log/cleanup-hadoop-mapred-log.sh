#!/bin/bash

# Directory where logs are stored (please change to the actual path)
LOG_DIR="<log-directory>"

# Move to the log directory
cd "$LOG_DIR" || exit 1

# 1. hadoop-mapred-historyserver-*.log.N - Delete logs older than 2 weeks
find . -type f -name "hadoop-mapred-historyserver-*.log.*" -mtime +14 -exec echo "deleting...: {}" \; -exec rm {} \;

# 2. hadoop-mapred-historyserver-*.out.N - Delete logs older than 2 weeks
find . -type f -name "hadoop-mapred-historyserver-*.out.*" -mtime +14 -exec echo "deleting...: {}" \; -exec rm {} \;
