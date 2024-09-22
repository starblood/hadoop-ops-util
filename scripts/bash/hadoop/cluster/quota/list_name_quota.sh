#!/bin/bash


user_list=($(hdfs dfs -ls /user/ | awk '{print $8}'))

for user in "${user_list[@]}"; do
    user_name=$(echo $user | cut -d '/' -f3)
    echo "$user_name       QUOTA       REM_QUOTA     SPACE_QUOTA REM_SPACE_QUOTA    DIR_COUNT   FILE_COUNT       CONTENT_SIZE PATHNAME"
    hdfs dfs -count -q -v -h $user | tail -n1
done
