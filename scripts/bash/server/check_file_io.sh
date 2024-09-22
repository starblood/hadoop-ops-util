#!/bin/bash
## NOTE: Run with root privileges
## A program to check which process is opening/writing/reading data on a specific partition

# DEV: i.e. /dev/sdm4
DEV=$1
# i.e. /data12/fileio.log
LOG_FILE_PATH=$2
# i.e. /home/hadoop/yarn/local/usercache
APP_CACHE_DIR=$3
while true
do
  # list open files. For more details: man lsof
  # lists on its standard output file information about files opened by processes
  lsof +f -- $DEV  | grep -v root >> $LOG_FILE_PATH
  echo "-------------------------------------------------" >> $LOG_FILE_PATH
  date >> $LOG_FILE_PATH
  df -h >> $LOG_FILE_PATH
  du -h --max-depth=1 $APP_CACHE_DIR >> $LOG_FILE_PATH
  echo "-------------------------------------------------" >> $LOG_FILE_PATH
  sleep 300
done
