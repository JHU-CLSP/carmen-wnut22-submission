# Changes in User Geolocation over Time: A Study with Carmen 2.0

Anonymized git repo for W-NUT 2022 submission.

## Carmen 2.0

An updated version of Carmen, a library for geolocating tweets.

Given a tweet, Carmen will return `Location` objects that represents a physical location.
The original version of Carmen used an internal location database, but we update and expand with GeoNames.

Install:

    $ python setup.py install

For Carmen front-end:

    $ python -m carmen.cli --help


### Geonames Mapping


## Experiments
Code for experiments is in `experiments`.

The script to evaluate Carmen with different location databases and target datasets is `evaluate_carmen.py`.
`batch_evaluate_carmen.py` is the same as `evaluate_carmen.py`, but written for a distributed Sun Grid Engine (SGE) setup.

These are the important options:
    - `--input-file`: JSONlines tweet file of tweets to geolocate
    - `--location-file`: path to Carmen location database to use

The other scripts are for collecting dataset statistics and to filter datasets for ablations (e.g., by language, year, etc.).

See paper for description of evaluation metrics and results.

