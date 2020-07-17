#!/bin/bash

./gradlew quarkusBuild

docker build -f src/main/docker/Dockerfile.jvm -t explorviz/reconstructor-service-jvm .

kind load docker-image explorviz/reconstructor-service-jvm:latest

kubectl apply --namespace=explorviz-dev -f manifest.yml