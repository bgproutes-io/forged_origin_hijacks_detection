#!/bin/bash
#
# Pull the latest version of all the docker images.

docker_name="unistrahijackdetection"

# Pull the database image.
docker pull $docker_name/type1_db
docker pull $docker_name/type1_newedge
docker pull $docker_name/type1_peeringdb
docker pull $docker_name/type1_bidirectionality
docker pull $docker_name/type1_topofeat
docker pull $docker_name/type1_aspathfeat
docker pull $docker_name/type1_newedge
docker pull $docker_name/type1_sampling