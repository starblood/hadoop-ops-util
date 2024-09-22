#!/bin/bash
# for delete old log files or etc

# find older than 30 days by modified time based
find . -mtime +30

# if you want to delete file older than 30 days, use below command
# find . -mtime +30 | sudo xargs rm -f
