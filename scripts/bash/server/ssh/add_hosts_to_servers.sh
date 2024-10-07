#!/bin/bash

# read hosts.txt file
hosts_file="$1"
# read server list from file
servers_file="$2"

# check server list file exists
if [ ! -f "$servers_file" ]; then
  echo "Error: $servers_file cannot find file."
  exit 1
fi

# check hosts.txt file exists
if [ ! -f "$hosts_file" ]; then
  echo "Error: $hosts_file cannot find file."
  exit 1
fi

# date format is YYYY-MM-DD
current_date=$(date +"%Y-%m-%d")

# backup file to /etc/hosts.bak-YYYY-MM-DD format
backup_file="/etc/hosts.bak-$current_date"

# read servers from servers_file file line by line
while IFS= read -r server; do
  echo "connecting server $server ..."

  # backup /etc/hosts file
  rsh -l irteamsu "$server" sudo cp /etc/hosts $backup_file < /dev/null

  # send hosts.txt to server as temporary file
  rcp $hosts_file irteamsu@$server:/home1/irteamsu/hosts_tmp.txt

  # add hosts_tmp.txt file contents to end end of /etc/hosts file
  rsh -l irteamsu "$server" "sudo sh -c 'cat /home1/irteamsu/hosts_tmp.txt >> /etc/hosts'" < /dev/null

  # print done message.
  echo "hosts.txt contents are added to $server's /etc/hosts."

  # delete temporary file
  rsh -l irteamsu "$server" rm /home1/irteamsu/hosts_tmp.txt < /dev/null

done < "$servers_file"

echo "all hosts are added to all servers' /etc/hosts file."
