#!/bin/bash

#$ -cwd -V  # Send your environment variables to the compute node
#$ -N bch-strm-d  # Name of the job
#$ -j y -o $JOB_NAME-$JOB_ID.out  # Name of the log file
#$ -M jzhan237@jhu.edu  # Put your email here
# #$ -m b  # Email notification for job start
# #$ -m e  # Email notification for job end (NEVER USE THIS FOR JOB ARRAYS YOU WILL GET AN EMAIL FOR EACH SUB-JOB)
# $ -l ram_free=10G,mem_free=10G  # How much memory we request from the compute node
# #$ -pe smp 10  # How many processes our job will spawn. The allocated resources (-l) are PER PROCESS
#$ -t 1-100  # How many tasks in our job array. Access task ID with $SGE_TASK_ID

#####
# database files
LOC_DEFAULT="/home/jzhan237/export/carmen-update/carmen-python/carmen/data/locations.json"
LOC_GEONAMES_COMBINED="/home/jzhan237/export/carmen-update/carmen-python/carmen/data/geonames_locations_combined.json"
LOC_GEONAMES_COMBINED_ENHANCED="/home/jzhan237/export/carmen-update/carmen-python/carmen/data/geonames_locations_combined_enhanced.json"
LOC_GEONAMES_ONLY="/home/jzhan237/export/carmen-update/carmen-python/carmen/data/geonames_locations_only.json"

CC_DEFAULT="/home/jzhan237/export/carmen-update/evaluation/c2c_dicts/locations_country2code.pkl"
CC_GEONAMES_COMBINED="/home/jzhan237/export/carmen-update/evaluation/c2c_dicts/geonames_locations_only_country2code.pkl"
CC_GEONAMES_ONLY="/home/jzhan237/export/carmen-update/evaluation/c2c_dicts/geonames_locations_only_country2code.pkl"

# geonames only
# LOC_FILE=$LOC_GEONAMES_ONLY
# LOC_FILE_STR="geonames_only"
# CC_DICT=$CC_GEONAMES_ONLY
# geonames combined
# LOC_FILE=$LOC_GEONAMES_COMBINED
# LOC_FILE_STR="geonames_combined"
# CC_DICT=$CC_GEONAMES_COMBINED
# combined enhanced
# LOC_FILE=$LOC_GEONAMES_COMBINED_ENHANCED
# LOC_FILE_STR="geonames_combined_enhanced"
# CC_DICT=$CC_GEONAMES_COMBINED
# default
LOC_FILE=$LOC_DEFAULT
LOC_FILE_STR="default"
CC_DICT=$CC_DEFAULT

DATA_FOLDER="/home/jzhan237/export/carmen-update/data/twitter-geo-stream"
INPUT_FILES=($DATA_FOLDER/*.gz)
STM_GEOTAG_OUTPUT="/home/jzhan237/export/carmen-update/evaluation/new_exp_stream_${LOC_FILE_STR}/"


OUTPUT_DIR=$STM_GEOTAG_OUTPUT
mkdir -p $OUTPUT_DIR
source activate carmen-env
NUM_TASKS=100 # WARNING: be consistent with -t 1-NUM_TASKS above!
#####

# Call your script and pass the subset of files assigned to this task
NUM_FILES=${#INPUT_FILES[*]}
echo "Total number of files:${NUM_FILES}" 
STEP=`python -c "from math import ceil; print(ceil($NUM_FILES/$NUM_TASKS))"`  # compute ceil($NUM_FILES/$NUM_TASKS)
START_INDEX=$[(SGE_TASK_ID - 1) * STEP]
# Access array subset for this task with ${INPUT_FILES[@]:$START_INDEX:$STEP}
echo "Started files ${START_INDEX} TO $[START_INDEX + STEP]..."
INPUT_BATCH=${INPUT_FILES[@]:$START_INDEX:$STEP}

#####
if [ -f "${OUTPUT_DIR}/${SGE_TASK_ID}.partial" ]
then
    echo "Task ${SGE_TASK_ID} cached. Not running it again."
elif [[ -z "${INPUT_BATCH// }" ]] # if input batch is empty
then
    echo "Task ${SGE_TASK_ID} has empty input. Skipped."
else
    time python batch_evaluate_carmen.py \
        --input-file ${INPUT_BATCH} \
        --output-path ${OUTPUT_DIR} \
        --location-file ${LOC_FILE} \
        --country-code-dict ${CC_DICT} \
        --task-id ${SGE_TASK_ID}
fi
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
