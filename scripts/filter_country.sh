#!/bin/bash

#$ -cwd -V  # Send your environment variables to the compute node
#$ -N filt-cntry
#$ -j y -o $JOB_NAME-$JOB_ID.out
#$ -M jzhan237@jhu.edu
#$ -m e
# #$ -l ram_free=8G,mem_free=8G

# Activate dev environments and call programs
conda activate carmen-env

GEO_STREAM_DIR="/home/jzhan237/export/carmen-update/data/twitter-geo-stream"
GEO_STREAM_DATASET=($GEO_STREAM_DIR/*.gz)

GEO_STREAM_US_SPLIT="/home/jzhan237/export/carmen-update/data/twitter-geo-stream-us-split"

python filter_country.py \
    --input-files ${GEO_STREAM_DATASET[@]} \
    --output-dir $GEO_STREAM_US_SPLIT
