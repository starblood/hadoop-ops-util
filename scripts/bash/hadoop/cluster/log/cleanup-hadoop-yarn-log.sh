#!/bin/bash

# Directory where logs are stored (please change to the actual path)
LOG_DIR="<yarn-log-dir>"

# Move to the log directory
cd "$LOG_DIR" || exit 1

# Current date (in seconds)
current_date=$(date +%s)

# Function to calculate the difference between two dates in days
datediff() {
    d1=$1
    d2=$2
    echo $(( (d1 - d2) / 86400 ))
}

# 1. SecurityAuth.audit.YYYY-MM-DD - Delete logs older than 1 week, rm-audit.log.2023-02-24
for file in rm-audit.log.*; do
    if [[ $file =~ rm\-audit\.log\.([0-9]{4}-[0-9]{2}-[0-9]{2}) ]]; then
        file_date_str=${BASH_REMATCH[1]}
        file_date=$(date -d "$file_date_str" +%s)
        diff_days=$(datediff $current_date $file_date)
        if (( diff_days > 7 )); then
            echo "deleting...: $file (age: $diff_days days)"
            rm "$file"
        fi
    fi
done

# 2. hadoop-yarn-resourcemanager-*.log.N - Delete logs older than 2 weeks
find . -type f -name "hadoop-yarn-resourcemanager-*log.*" -mtime +14 -exec echo "deleting...: {}" \; -exec rm {} \;
