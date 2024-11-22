#!/bin/bash

# You'll need a profile (esgf2) managed by vault for account 730335463484
aws ecr get-login-password --profile esgf2 --region us-east-1 | docker login --username AWS --password-stdin 730335463484.dkr.ecr.us-east-1.amazonaws.com

docker build --platform=linux/x86_64 -f ../Dockerfile -t esgf-west-discovery:latest ..

docker tag esgf-west-discovery:latest 730335463484.dkr.ecr.us-east-1.amazonaws.com/esgf-west-discovery:latest

docker push 730335463484.dkr.ecr.us-east-1.amazonaws.com/esgf-west-discovery:latest

aws ecs update-service --force-new-deployment --service esgf-west-discovery-service --cluster esgf-west-discovery --profile esgf2