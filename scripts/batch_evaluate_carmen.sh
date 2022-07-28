#!/bin/bash

#$ -cwd -V  # Send your environment variables to the compute node
#$ -N bch-strm  # Name of the job
#$ -j y -o $JOB_NAME-$JOB_ID.out  # Name of the log file
#$ -M jzhan237@jhu.edu  # Put your email here
# #$ -m b  # Email notification for job start
# #$ -m e  # Email notification for job end (NEVER USE THIS FOR JOB ARRAYS YOU WILL GET AN EMAIL FOR EACH SUB-JOB)
#$ -l ram_free=25G,mem_free=25G  # How much memory we request from the compute node
# #$ -pe smp 10  # How many processes our job will spawn. The allocated resources (-l) are PER PROCESS
#$ -t 1-100  # How many tasks in our job array. Access task ID with $SGE_TASK_ID

#####
DEBUG_INPUTS="/home/jzhan237/export/carmen-update/data/twitter-stream-geotag/debug_split"
DEBUG_OUTPUT="/home/jzhan237/export/carmen-update/evaluation/exp_stream_geotag_debug_split"

# stream-geotag
AFR_INP="/home/jzhan237/export/carmen-update/data/twitter-stream-geotag/afr_0.001_geotag_out"
BRA_INP="/home/jzhan237/export/carmen-update/data/twitter-stream-geotag/bra_0.001_geotag_out"
USC_INP="/home/jzhan237/export/carmen-update/data/twitter-stream-geotag/uscan_0.001_geotag_out"
AFR_FILES=($AFR_INP/*.gz)
BRA_FILES=($BRA_INP/*.gz)
USC_FILES=($USC_INP/*.gz)
INPUT_FILES=("${AFR_FILES[@]}" "${BRA_FILES[@]}" "${USC_FILES[@]}")
STM_GEOTAG_OUTPUT="/home/jzhan237/export/carmen-update/evaluation/exp_stream_geotag"

OUTPUT_DIR=$STM_GEOTAG_OUTPUT
mkdir -p $OUTPUT_DIR
source activate carmen-env
NUM_TASKS=100 # WARNING: be consistent with -t 1-NUM_TASKS above!
#####

# database files
LOC_DEFAULT="/home/jzhan237/export/carmen-update/carmen-python/carmen/data/locations.json"
LOC_GEONAMES_COMBINED="/home/jzhan237/export/carmen-update/carmen-python/carmen/data/geonames_locations_combined.json"
LOC_GEONAMES_ONLY="/home/jzhan237/export/carmen-update/carmen-python/carmen/data/geonames_locations_only.json"

LOC_FILE=$LOC_GEONAMES_ONLY

# Call your script and pass the subset of files assigned to this task
NUM_FILES=${#INPUT_FILES[*]}
STEP=`python -c "from math import ceil; print(ceil($NUM_FILES/$NUM_TASKS))"`  # compute ceil($NUM_FILES/$NUM_TASKS)
START_INDEX=$[(SGE_TASK_ID - 1) * STEP]
# Access array subset for this task with ${INPUT_FILES[@]:$START_INDEX:$STEP}
echo "Started files ${START_INDEX} TO $[START_INDEX + STEP]..."
INPUT_BATCH=${INPUT_FILES[@]:$START_INDEX:$STEP}

#####
time python batch_evaluate_carmen.py \
    --input-file ${INPUT_BATCH} \
    --output-path ${OUTPUT_DIR} \
    --location-file ${LOC_FILE}
#####

# Check exit status of task
status=$?
if [ $status -ne 0 ]
then
    echo "Task $SGE_TASK_ID failed"
    exit 1
fi

# Use the last task to "reduce" the output
if [ ${SGE_TASK_ID} -eq ${SGE_TASK_LAST} ]
then
    # Ensure that last task ID is the last task to finish
    while [ $(qstat -u $USER | grep ${JOB_ID} | wc -l) -ne 1 ]
    do
        # Wait patiently
        sleep 20
    done

##### optional aggregation
    time python batch_evaluate_carmen.py --output-path $OUTPUT_DIR --aggregate
#####

    echo "Done"
fi
