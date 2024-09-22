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

# 1. SecurityAuth.audit.YYYY-MM-DD - Delete logs older than 1 week
for file in SecurityAuth.audit.*; do
    if [[ $file =~ SecurityAuth\.audit\.([0-9]{4}-[0-9]{2}-[0-9]{2}) ]]; then
        file_date_str=${BASH_REMATCH[1]}
        file_date=$(date -d "$file_date_str" +%s)
        diff_days=$(datediff $current_date $file_date)
        if (( diff_days > 7 )); then
            echo "deleting...: $file (age: $diff_days days)"
            rm "$file"
        fi
    fi
done

# 2. gc.log-YYYYMMDDHHMM - Delete logs older than 1 month
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

# 3. hadoop-hdfs-namenode-*.log.N - Delete logs older than 2 weeks
find . -type f -name "hadoop-hdfs-namenode-*.log.*" -mtime +14 -exec echo "deleting...: {}" \; -exec rm {} \;

# 4. hadoop-hdfs-zkfc-*.out.N - Delete logs older than 2 weeks
find . -type f -name "hadoop-hdfs-zkfc-*.out.*" -mtime +14 -exec echo "deleting...: {}" \; -exec rm {} \;

# 5. hadoop-hdfs-zkfc-*.log.N - Delete logs older than 2 weeks
find . -type f -name "hadoop-hdfs-zkfc-*.log.*" -mtime +14 -exec echo "deleting...: {}" \; -exec rm {} \;

# 6. hadoop-hdfs-journalnode-*.log.N - Delete logs older than 2 weeks
find . -type f -name "hadoop-hdfs-journalnode-*.log.*" -mtime +14 -exec echo "deleting...: {}" \; -exec rm {} \;

# 7. hdfs-audit.log.YYYY-MM-DD - Delete logs older than 1 week
for file in hdfs-audit.log.*; do
    if [[ $file =~ hdfs-audit\.log\.([0-9]{4}-[0-9]{2}-[0-9]{2}) ]]; then
        file_date_str=${BASH_REMATCH[1]}
        file_date=$(date -d "$file_date_str" +%s)
        diff_days=$(datediff $current_date $file_date)
        if (( diff_days > 7 )); then
            echo "deleting...: $file (age: $diff_days days)"
            rm "$file"
        fi
    fi
done

# 8. hs_err_pid - Delete logs older than 2 weeks
find . -type f -name "hs_err_pid*" -mtime +14 -exec echo "deleting...: {}" \; -exec rm {} \;
