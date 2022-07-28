#!/bin/bash

#$ -cwd -V  # Send your environment variables to the compute node
#$ -N carmen-dict
#$ -j y -o $JOB_NAME-$JOB_ID.out
#$ -M jzhan237@jhu.edu
#$ -m e
#$ -l ram_free=32G,mem_free=32G

# Activate dev environments and call programs
conda activate carmen-env

LOC_DEFAULT="/home/jzhan237/export/carmen-update/carmen-python/carmen/data/locations.json"
LOC_GEONAMES_COMBINED="/home/jzhan237/export/carmen-update/carmen-python/carmen/data/geonames_locations_combined.json"
LOC_GEONAMES_ONLY="/home/jzhan237/export/carmen-update/carmen-python/carmen/data/geonames_locations_only.json"

python get_country_to_code_dict.py -i $LOC_DEFAULT $LOC_GEONAMES_COMBINED $LOC_GEONAMES_ONLY -o c2c_dicts