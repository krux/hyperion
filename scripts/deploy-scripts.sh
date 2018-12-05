#!/bin/bash

# ex: deploy.sh s3://xxx/scripts

S3_URI=$1

aws s3 cp . $S3_URI --recursive --exclude "deploy-scripts.sh"
