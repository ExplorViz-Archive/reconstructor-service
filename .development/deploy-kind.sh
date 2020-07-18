#!/bin/bash

./gradlew quarkusBuild

docker build -f src/main/docker/Dockerfile.jvm -t explorviz/reconstructor-service-jvm .

kind load docker-image --name explorviz-dev explorviz/reconstructor-service-jvm:latest

kubectl apply --context kind-explorviz-dev -f manifest.yml