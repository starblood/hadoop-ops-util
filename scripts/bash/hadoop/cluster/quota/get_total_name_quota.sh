#!/bin/bash
# Calculate the total allocated name quota from the name_quota_list.txt file produced by list_name_quota.sh

name_quota_list_file="$1"

quota_summaries=($(cat $name_quota_list_file | grep '/user/' | awk '{print $1$2}'))

total=0
for quota_summary in "${quota_summaries[@]}"; do
    line=$(echo "$quota_summary" | grep -v 'noneinf')
    # remove empty line
    if [[ ! -z "$line" ]]; then
        quota_size=$(echo $line)
        echo "$quota_size"
        size=$(echo "$quota_size" | sed 's/[^0-9.]//g')
        unit=$(echo "$quota_size" | sed 's/[0-9.]//g')

        if [ "$unit" == "M" ]; then
            # Convert M to K (1M = 1024K)
            size=$(echo "$size * 1024" | bc)
        fi
        total=$(echo "$total + $size" | bc)
    fi
done

mbytes_total=$(echo "scale=2; $total / 1024" | bc)

echo "total assigned quota: $mbytes_total M bytes"
