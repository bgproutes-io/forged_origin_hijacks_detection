#!/bin/bash


docker_name="unistrahijackdetection"


# Build base image.
docker login -u $docker_name
name="dfoh_aspathfeat"
docker buildx build --tag=$name -f docker/Dockerfile .

docker tag ${name} ${docker_name}/${name}
docker push $docker_name/$name

