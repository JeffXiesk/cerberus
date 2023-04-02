


# if [ "$1" = "-i" ]; then
#     echo '111'
#     shift
#     if [ "$1" = "-o" ]; then
#         echo '222'
#         shift
#     fi
#     if [ "$1" = "-p" ]; then
#         echo '333'
#         shift
#     fi
# fi

#!/bin/bash
# in deploy folder run.
ssh_options_cloud='-i scripts/cloud-deploy/bft11.pem -o StrictHostKeyChecking=no -o LogLevel=ERROR -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=60'

# source shutdown_instances.sh
# num=$(python scripts/cloud-deploy/pyscript/find_insnum.py)
# num_arr=(`echo $num | tr ',' ' '`)
# totalnum=${num_arr[2]}
# client_num=${num_arr[0]}
# peer_num=${num_arr[1]}
# # echo $totalnum
# # echo $client_num
# # echo $peer_num

# if [ "$1" = "-i" ]; then
#     echo "Init"
#     shift
#     if [ "$1" = "-r" ]; then
#         shift
#         # totalnum=6
#         for ((c=1;c<=$totalnum;c++))    
#         do
#             new_instance_info=$(aws ec2 run-instances \
#             --launch-template LaunchTemplateId=lt-0854465890b2cf8e9 \
#             --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value="test"}]')
#         done
#             # --launch-template LaunchTemplateId=lt-0052a4dd500c7b168 \
#             # --launch-template LaunchTemplateId=lt-0f484f5c1d4158d0e \

#         echo "sleep 60 seconds"
#         sleep 60
#     else
#         sleep 0.1
#     fi
#     public_ip=$(
#         aws ec2 describe-instances   \
#         --filter "Name=network-interface.status,Values=available,in-use"   \
#         --query "Reservations[*].Instances[*].PublicIpAddress"   \
#         --output=text)
#     echo $public_ip

#     private_ip=$(
#         aws ec2 describe-instances   \
#         --filter "Name=network-interface.status,Values=available,in-use"   \
#         --query "Reservations[*].Instances[*].PrivateIpAddress"   \
#         --output=text)
#     echo $private_ip

#     write_result=$(python scripts/cloud-deploy/pyscript/write_cloud_instance.py $client_num $peer_num $public_ip $private_ip)
#     echo $write_result

#     # show info of instance
#     public_ip_arr=(`echo $public_ip | tr ',' ' '`)
#     for i in "${public_ip_arr[@]}"
#     do
#        echo "$i"
#     done

#     private_ip_arr=(`echo $private_ip | tr ',' ' '`)
#     for i in "${private_ip_arr[@]}"
#     do
#        echo "$i"
#     done

#     # set root login, reference : https://www.youtube.com/watch?v=xE_oaWVhaV4
#     echo "Start set root login..."
#     for i in "${public_ip_arr[@]}"
#     do
#         # send local 'sshd_config' ssh config file to instance
#         scp $ssh_options_cloud scripts/cloud-deploy/sshd_config ubuntu@$i:~
#         # set root login
#         ssh $ssh_options_cloud ubuntu@$i "sudo cp ~/.ssh/authorized_keys /root/.ssh/authorized_keys;sudo cp ~/sshd_config /etc/ssh/sshd_config"
#         echo "$i set root login done..."
#     done
#     echo "End set root login..."

#     echo "Start set ssh key..."
#     for i in "${public_ip_arr[@]}"
#     do
#         # send local 'sshd_config' ssh config file to instance
#         scp $ssh_options_cloud 'scripts/cloud-deploy/key/id_rsa' root@$i:/root/.ssh
#         scp $ssh_options_cloud 'scripts/cloud-deploy/key/id_rsa.pub' root@$i:/root/.ssh
#         ssh $ssh_options_cloud root@$i 'chmod 600 /root/.ssh/id_rsa;chmod 600 /root/.ssh/id_rsa.pub;echo ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC3JeK5VQ3cRMLp5nHeMgIDTbbOvytBR6BDy4TK0QOqzyrGIlaSt966JkTsUfxXLw7Gc/cGRwpjVcszE3nGEvcquAEHuFOfYmt8Pat3cHuLgH4p/GPwBMbvKgrLNGrkRphFugK30IPN5yRvsUhpVzi/XJJN6iL68fRzdFzmOjQWgvmOcWPTVy7VV0GjX3XoO5XcmQU3/B52nZotypxCmDN91eJyNeVjpGgDdwT+Pc6eqr1yAx4PH/PDPOSQlrFC7x8zsuiwz+F+cLaUyVNmp5G/NSzcNoYKbxohnj11JVdVgnUj/CocG9dJjpxY4+NSCAaIRJ5kczF+9VVrzfhyId4D niu@niu-Standard-PC-i440FX-PIIX-1996 >> /root/.ssh/authorized_keys'
#         echo "$i sent ssh key done..."
#     done
#     echo "End set ssh key..."
# else
#     echo "Not init"
# fi

# # echo "$public_ip_arr"
# # echo "${public_ip_arr[@]:1:4}"

# for i in "${public_ip_arr[@]:0:totalnum}"
# do
#     scp $ssh_options_cloud root@$i:/root/experiment-output-* scripts/cloud-deploy/experiment-output
#     echo "$i fetch experiment done..."
# done


scp $ssh_options_cloud root@100.25.211.118:/root/experiment-output-* scripts/cloud-deploy/experiment-output
echo "$i fetch experiment done..."

# for each_region in ${AWS_REGIONS} ; do 
#     aws ec2 import-key-pair \
#     --key-name MyKeyPair \
#     --public-key-material fileb://$HOME/.ssh/id_rsa_MyKeyPair.pub \
#     --region $each_region ; 
# done


for tar in scripts/cloud-deploy/experiment-output/*.tar.gz;  do 
    tar -zxvf $tar -C scripts/cloud-deploy/experiment-output/;
done