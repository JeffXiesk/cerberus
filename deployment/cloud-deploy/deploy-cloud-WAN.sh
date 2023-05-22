#!/bin/bash

private_key_file="/opt/gopath/src/github.com/IBM/mirbft/deployment/key/id_rsa"
ssh_options="-i $private_key_file -o StrictHostKeyChecking=no -o ServerAliveInterval=60"


public_ip=$(cat cloud-instance.info | awk '{ print $2}')
public_ip_arr=(`echo $public_ip | tr ',' ' '`)

echo ${public_ip_arr[@]}

if [ "$1" = "-i" ]; then
    echo "Init..."
    shift
    # set root login, reference : https://www.youtube.com/watch?v=xE_oaWVhaV4
    echo "Start set root login..."
    for i in "${public_ip_arr[@]}"
    do
        # send local 'sshd_config' ssh config file to instance
        echo $i
        scp $ssh_options sshd_config ubuntu@$i:~ &
    done
    wait

    for i in "${public_ip_arr[@]}"
    do
        # set root login
        ssh $ssh_options ubuntu@$i "sudo cp ~/.ssh/authorized_keys /root/.ssh/authorized_keys;sudo cp ~/sshd_config /etc/ssh/sshd_config;sudo service sshd restart" &
    done
    wait

    echo "End set root login..."

    echo "Start set ssh key..."

    for i in "${public_ip_arr[@]}"
    do
        # send local 'sshd_config' ssh config file to instance
        scp $ssh_options 'key/id_rsa' root@$i:/root/.ssh &
        scp $ssh_options 'key/id_rsa.pub' root@$i:/root/.ssh &
        echo "$i sent ssh key done..."
    done
    wait

    for i in "${public_ip_arr[@]}"
    do
        ssh $ssh_options root@$i 'chmod 600 /root/.ssh/id_rsa;chmod 600 /root/.ssh/id_rsa.pub;echo ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC3JeK5VQ3cRMLp5nHeMgIDTbbOvytBR6BDy4TK0QOqzyrGIlaSt966JkTsUfxXLw7Gc/cGRwpjVcszE3nGEvcquAEHuFOfYmt8Pat3cHuLgH4p/GPwBMbvKgrLNGrkRphFugK30IPN5yRvsUhpVzi/XJJN6iL68fRzdFzmOjQWgvmOcWPTVy7VV0GjX3XoO5XcmQU3/B52nZotypxCmDN91eJyNeVjpGgDdwT+Pc6eqr1yAx4PH/PDPOSQlrFC7x8zsuiwz+F+cLaUyVNmp5G/NSzcNoYKbxohnj11JVdVgnUj/CocG9dJjpxY4+NSCAaIRJ5kczF+9VVrzfhyId4D niu@niu-Standard-PC-i440FX-PIIX-1996 >> /root/.ssh/authorized_keys' &
        echo "$i sent ssh key done..."
    done
    wait
fi

if [ "$1" = "-s" ]; then
    shift
    for i in "${public_ip_arr[@]}"
    do
        scp $ssh_options '/opt/gopath/src/github.com/IBM/mirbft/deployment/setup.sh' root@$i:/root
        ssh $ssh_options root@$i 'source /root/setup.sh' &
        echo "$i sent ssh key done..."
    done
    wait
fi

echo "End set ssh key..."
