#!/bin/bash

# docker_name="unistrahijackdetection"

# docker login -u $docker_name

# # Build base image.
# name="type1_bidirectionality_base"
# docker build --tag=$name base
# docker tag ${name} ${docker_name}/${name}
# docker push $docker_name/$name

# # Build peeringdb image.
# name="type1_bidirectionality"
# # docker build  --tag=$name peeringdb
# docker build --no-cache --tag=$name bidirectionality
# echo 'docker tag.'
# docker tag ${name} ${docker_name}/${name}
# echo 'docker push.'
# docker push $docker_name/$name


#!/bin/bash


docker_name="unistrahijackdetection"


# Build base image.
docker login -u $docker_name
name="dfoh_bidirectionality"
docker buildx build --tag=$name -f docker/Dockerfile .

docker tag ${name} ${docker_name}/${name}
docker push $docker_name/$name