#!/bin/bash


docker_name="unistrahijackdetection"

docker login -u $docker_name

# Build base image.
name="dfoh_sampling"
docker buildx build --tag=$name -f docker/Dockerfile .
docker tag ${name} ${docker_name}/${name}
docker push $docker_name/$name

