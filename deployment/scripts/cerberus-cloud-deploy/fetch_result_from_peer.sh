#!/bin/bash

peer_list=($(python3 scripts/cloud-deploy/pyscript/find_peer.py))

for peer in "${peer_list[@]}"
do
    echo $peer
    scp -r -i scripts/cloud-deploy/key/id_rsa -o StrictHostKeyChecking=no -o LogLevel=ERROR -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=60 root@$peer:/root/experiment-output . &
done
wait

echo "Fetch Over."