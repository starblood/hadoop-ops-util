#!/bin/bash

# Directory where logs are stored (please change to the actual path)
LOG_DIR="<log-directory>"

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

# 1. gc.log-YYYYMMDDHHMM - Delete logs older than 1 month
for file in gc.log-*; do
    if [[ $file =~ gc\.log\-([0-9]{12}) ]]; then
        file_date_str=${BASH_REMATCH[1]}
        file_date=$(date -d "${file_date_str:0:8} ${file_date_str:8:2}:${file_date_str:10:2}" +%s)
        diff_days=$(datediff $current_date $file_date)
        if (( diff_days > 30 )); then
            echo "deleting...: $file (age: $diff_days days)"
            rm "$file"
        fi
    fi
done

# 2. collector-gc.log-YYYYMMDDHHMM - Delete logs older than 1 month
for file in collector-gc.log-*; do
    if [[ $file =~ collector\-gc\.log\-([0-9]{12}) ]]; then
        file_date_str=${BASH_REMATCH[1]}
        file_date=$(date -d "${file_date_str:0:8} ${file_date_str:8:2}:${file_date_str:10:2}" +%s)
        diff_days=$(datediff $current_date $file_date)
        if (( diff_days > 30 )); then
            echo "deleting...: $file (age: $diff_days days)"
            rm "$file"
        fi
    fi
done

# 3. hbase-ams-master-*.log.N - Delete logs older than 2 weeks
find . -type f -name "hbase-ams-master-*.log.*" -mtime +14 -exec echo "deleting...: {}" \; -exec rm {} \;

# 4. ambari-metrics-collector.log.N - Delete logs older than 2 weeks
find . -type f -name "ambari-metrics-collector.log.*" -mtime +14 -exec echo "deleting...: {}" \; -exec rm {} \;
