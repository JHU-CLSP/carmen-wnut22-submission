# Changes in User Geolocation over Time: A Study with Carmen 2.0

## Obtaining the data

Due to Twitter's policies, only Tweet IDs, but not the actual content, can be directly released. Thus, we provide the Tweet IDs for the released datasets in the `data/` folder. We refer reader to popular tools such as the Twitter [Hydrator](https://github.com/DocNow/hydrator) to get access to the actual Tweet JSONlines files. Once Tweets are hydrated, please store the JSONlines files in `.gz` compressed format (preferably multiple `.gz` files to enable batch processing).

## Reproducing paper results
Run the Python scripts in the `evaluation/` folder with the corresponding bash script in the `script/` folder, using different location databases in the `database/` folder

## Directory contents
- `carmen` contains the Carmen 2.0 code (based off the original Carmen)
- `data/` contains Tweet IDs for different released datasets, including different splits of **Twitter-Global**. (Request data from authors)
- `database/` contains different location database that can be used to initialize Carmen.
    - `locations.json` is the original Carmen location database
    - `geonames_locations_only.json` is the new location database derived from the GeoNames databse
    - `geonames_locations_combined.json` is the combined version of `locations.json` and `geonames_locations_only.json`, with entries in `locations.json` mapped to a GeoNames entry, and then converted to the Carmen database format
- `evaluation/` contains main Python scripts that computes the performance of Carmen 2.0 across different datasets
- `preprocessing/` contains code to filter **Twitter-Global** into different splits. Since we already provided the splitted **Twitter-Global** Tweet IDs, it is likely that user can skip this preprocessing step.
- `scripts/` contains bash scripts to run all the other Python scripts provided in other folders. Note that these scripts only works on a server with Sun Grid Engine (SGE) queueing system, which is used for efficient batch processing on 100 CPU jobs. User need to adapt the input and output path of these scripts, and also adapt the batch processing part if not using SGE.
- `utils/` contains useful shortcuts for collecting results, e.g. format results into a csv table.
