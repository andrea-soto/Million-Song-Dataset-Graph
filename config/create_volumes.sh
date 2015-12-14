#!/usr/bin/env bash

# =============================================
# RUN SCRIPT AS ROOT USER
# Attaches 2 volumes to this instance: a Graph Volume of 200 GB mounted to /graph
# and a MSD Volume 280GB from snap-5178cf30 with the entire dataset mounted to /msong_dataset

cd ~

# ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
# Save instance info in environment variables
# ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

# Get instance id
INSTANCE_ID=$(ec2-metadata -i | cut -d:  -f2| cut -d' ' -f2)
export INSTANCE_ID
# Get instance public hostname
INSTANCE_PDNS=$(ec2-metadata -p | cut -d:  -f2| cut -d' ' -f2)
export INSTANCE_PDNS
# Get instance availability zone
INSTANCE_ZONE=$(ec2-metadata -z | cut -d:  -f2| cut -d' ' -f2)
export INSTANCE_ZONE

#echo 'export INSTANCE_ID='$INSTANCE_ID >> ~/.bashrc
#echo 'export INSTANCE_PDNS='$INSTANCE_PDNS >> ~/.bashrc
#echo 'export INSTANCE_ZONE='$INSTANCE_ZONE >> ~/.bashrc
#source ~/.bashrc

# ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
# Create Volumes
# ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

mkdir -p aws-info
 
### Create volume to store graph
echo LOG: Creating graph volume...
aws ec2 create-volume --size 200 --availability-zone $INSTANCE_ZONE --volume-type gp2 > aws-info/graph-volume.json
wait
GRAPH_VOL_ID=$(jq '.VolumeId' aws-info/graph-volume.json)
GRAPH_VOL_ID="${GRAPH_VOL_ID%\"}"
GRAPH_VOL_ID="${GRAPH_VOL_ID#\"}"
export GRAPH_VOL_ID

### Create volume from AWS snapshot of Million Song Dataset (full dataset)
echo LOG: Copying Million Song Dataset volume...
aws ec2 create-volume --availability-zone $INSTANCE_ZONE \
--snapshot-id snap-5178cf30 --volume-type gp2 > aws-info/msd-volume.json
wait
MSD_VOL_ID=$(jq '.VolumeId' aws-info/msd-volume.json)
MSD_VOL_ID="${MSD_VOL_ID%\"}"
MSD_VOL_ID="${MSD_VOL_ID#\"}"
export MSD_VOL_ID

echo LOG: Wait for volumes to become available...

sleep 30

# ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
# Attache volumes to this instance
# ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    
echo LOG: Attaching graph volumne...
aws ec2 attach-volume --volume-id $GRAPH_VOL_ID --instance-id $INSTANCE_ID --device /dev/xvdh

echo LOG: Attaching Million Song Dataset volume...
aws ec2 attach-volume --volume-id $MSD_VOL_ID --instance-id $INSTANCE_ID --device /dev/xvdj

echo LOG: Wait for volumes to be attached...
    
sleep 30 

# ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
# Mount volumes to instance
# ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

mkdir -p graph
sudo mkfs -t ext4 /dev/xvdh
sudo mount -t ext4 /dev/xvdh /graph

mkdir -p msong_dataset
sudo mount /dev/xvdj /msong_dataset