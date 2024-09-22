#!/bin/bash

hosts_file=$1
public_key_file="$2"
public_key_file_name=$(echo $public_key_file | cut -d '/' -f2)

while IFS= read -r line; do
    host="$line"
    # upload public key file
    echo "upload $public_key_file to $host"
    scp "$public_key_file" <user>@"$host":/tmp/$public_key_file_name
    scp add_authorized_keys.sh <user>@"$host":/tmp/add_authorized_keys.sh

    echo "set $host ~/.ssh/authorized_keys"
    ssh <user>@"$host" /bin/bash /tmp/add_authorized_keys.sh ~/.ssh/authorized_keys /tmp/$public_key_file_name
done < "$hosts_file"
