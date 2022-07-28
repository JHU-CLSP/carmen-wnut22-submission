#!/bin/bash

#$ -cwd -V  # Send your environment variables to the compute node
#$ -N bch-strm  # Name of the job
#$ -j y -o $JOB_NAME-$JOB_ID.out  # Name of the log file
#$ -M jzhan237@jhu.edu  # Put your email here
# #$ -m b  # Email notification for job start
# #$ -m e  # Email notification for job end (NEVER USE THIS FOR JOB ARRAYS YOU WILL GET AN EMAIL FOR EACH SUB-JOB)
#$ -l ram_free=25G,mem_free=25G  # How much memory we request from the compute node
# #$ -pe smp 10  # How many processes our job will spawn. The allocated resources (-l) are PER PROCESS
#$ -t 1-1  # How many tasks in our job array. Access task ID with $SGE_TASK_ID

#####
TWITTER_WORLD_TEST="/home/jzhan237/export/carmen-update/data/twitter-world/world_tweets_test.json"
INPUT_FILES=$TWITTER_WORLD_TEST
EXP_WORLD_BY_YEAR_TEST="/home/jzhan237/export/carmen-update/evaluation/exp_world_by_year_test"

OUTPUT_DIR=$EXP_WORLD_BY_YEAR_TEST
mkdir -p $OUTPUT_DIR
source activate carmen-env
NUM_TASKS=1 # WARNING: be consistent with -t 1-NUM_TASKS above!
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
# INPUT_BATCH=${INPUT_FILES[@]:$START_INDEX:$STEP}
INPUT_BATCH=$INPUT_FILES

#####
time python batch_evaluate_coverage.py \
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
    time python batch_evaluate_coverage.py --output-path $OUTPUT_DIR --aggregate
#####

    echo "Done"
fi
