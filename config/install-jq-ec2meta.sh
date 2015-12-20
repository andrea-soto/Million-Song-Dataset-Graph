#!/usr/bin/env bash

# Install jq to parse JSON in shell
sudo yum install jq

# Install EC2 Instance Metadata Query Tool
wget http://s3.amazonaws.com/ec2metadata/ec2-metadata
chmod a+x ec2-metadata
mv ec2-metadata /usr/bin