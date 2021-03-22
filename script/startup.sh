export INSTANCE_REGION=europe-west3
export INSTANCE_ZONE=europe-west3-a
export PROJECT_NAME=helm
export CLUSTER_NAME=${PROJECT_NAME}-cluster

gcloud container clusters create ${CLUSTER_NAME} \
    --zone ${INSTANCE_ZONE} \
    --scopes cloud-platform \
    --enable-autorepair \
    --enable-autoupgrade \
    --enable-autoscaling --min-nodes 1 --max-nodes 4 \
    --num-nodes 3

gcloud container clusters get-credentials ${CLUSTER_NAME} \
    --zone ${INSTANCE_ZONE}

kubectl cluster-info

kubectl create clusterrolebinding cluster-admin-binding \
    --clusterrole=cluster-admin --user=$(gcloud config get-value account)
