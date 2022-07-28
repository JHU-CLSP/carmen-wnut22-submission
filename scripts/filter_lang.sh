#!/bin/bash

#$ -cwd -V  # Send your environment variables to the compute node
#$ -N filt-lang
#$ -j y -o $JOB_NAME-$JOB_ID.out
#$ -M jzhan237@jhu.edu
#$ -m e
# #$ -l ram_free=8G,mem_free=8G

# Activate dev environments and call programs
conda activate carmen-env

GEO_STREAM_DIR="/home/jzhan237/export/carmen-update/data/twitter-geo-stream"
GEO_STREAM_DATASET=($GEO_STREAM_DIR/*.gz)

GEO_STREAM_EN_SPLIT="/home/jzhan237/export/carmen-update/data/twitter-geo-stream-en-split"

python filter_lang.py \
    --input-files ${GEO_STREAM_DATASET[@]} \
    --output-dir $GEO_STREAM_EN_SPLIT
