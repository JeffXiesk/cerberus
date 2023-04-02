# bandwidth=(5)
# bandwidth=(300 200 20)

# len=${#bandwidth[@]}

bash scripts/cloud-deploy/deploy-cloud-WAN.sh -i -k
for ((i=1;i<=5;i++)); do
    echo '======'
    # echo "cp scripts/experiment-configuration/generate-config-$i.sh scripts/experiment-configuration/generate-config.sh"
    cp scripts/experiment-configuration/generate-config-$i.sh scripts/experiment-configuration/generate-config.sh
    echo "source scripts/cloud-deploy/deploy-cloud.sh -d"
    bash scripts/cloud-deploy/deploy-cloud-WAN.sh -d
    echo '======'
done

bash scripts/cloud-deploy/deploy-cloud-WAN.sh -sd
