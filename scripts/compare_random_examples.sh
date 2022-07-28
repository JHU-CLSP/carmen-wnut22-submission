#!/bin/bash

#$ -cwd -V  # Send your environment variables to the compute node
#$ -N rand-ex
#$ -j y -o $JOB_NAME-$JOB_ID.out
#$ -M jzhan237@jhu.edu
#$ -m e
# #$ -l ram_free=32G,mem_free=32G

# Activate dev environments and call programs
conda activate carmen-env

NUM=10
OUTPUT_DIR="/home/jzhan237/export/carmen-update/evaluation/compare_random_examples_out_${NUM}"

time python compare_random_examples.py \
    --data_dir /home/jzhan237/export/carmen-update/data/twitter-geo-stream \
    --num $NUM \
    --output_dir $OUTPUT_DIR \
    --pos geo-c \
    --neg default \
    --strong \
    --ext .gz

echo "Done"
