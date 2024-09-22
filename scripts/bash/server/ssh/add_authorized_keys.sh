#!/bin/bash

authorized_keys_file=$1
public_key_file=$2

if [[ ! -e $public_key_file ]]; then
    echo "public key not found."
    exit 1
fi

if [[ ! -f $authorized_keys_file ]]; then
    authorized_keys_content=""
else
    authorized_keys_content="$(cat $authorized_keys_file)"
fi

public_key_file_content="$(cat $public_key_file)"

if [[ -f $authorized_keys_file ]]; then
    mv $authorized_keys_file /tmp/authorized_keys.origin
fi

touch /tmp/authorized_keys

if [[ ! -z $authorized_keys_content ]]; then
    echo "$authorized_keys_content" > /tmp/authorized_keys
fi

echo "$public_key_file_content" >> /tmp/authorized_keys

sudo cp -f /tmp/authorized_keys ~/.ssh/authorized_keys
sudo chmod 600 ~/.ssh/authorized_keys
sudo chown <user>:<group> ~/.ssh/authorized_keys
