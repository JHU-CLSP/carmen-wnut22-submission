"""
Evaluate the accuracy of Carmen

Author: Alexandra DeLucia, Jack Zhang
"""
import carmen
from littlebird import TweetReader
import argparse
import logging
import pickle
import numpy as np
import pandas as pd
import time
import os

from geopy import Point
from geopy.distance import distance as geopy_distance

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


class DistQuantile:
    def __init__(self, dists, quant_step=0.001) -> None:
        '''
        dists: a list of real numbers indicating distances
        quant_step: granularity
        '''
        dists_df = pd.DataFrame(np.array(dists), columns=['distance'])
        self.quant_df = dists_df.quantile(np.arange(0,1+quant_step,quant_step))

    def get_quant(self, dist):
        return self.quant_df.iloc[(self.quant_df - dist).abs()['distance'].argmin()].name


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--aggregate", action="store_true")
    parser.add_argument("--input-file", nargs="+", type=str, default="../data/dev_set.gz", help="Tweets in JSONlines format (.gz format allowed)")
    parser.add_argument("--output-path", type=str, default=".", help="Path for output csv")
    parser.add_argument("--location-file", type=str, default="../carmen-python/carmen/data/locations.json")
    parser.add_argument("--resolvers", nargs="+", default=None, help="Limit Carmen to specific resolver")
    parser.add_argument("--verbose", "-v", action="store_true", help="Print details")
    parser.add_argument("--store-dists", action="store_true", help="Store detailed distances between ground truth and resolved location")
    parser.add_argument("--dists-filename", default="dists.txt", help="Output filename storing the distances")
    parser.add_argument("--sort-dists-tweets", default=None, help="Sort resolved tweets by descending dists. Enter a filename for resolved tweets")
    parser.add_argument("--country-code-dict", type=str, default="/export/b10/jzhan237/carmen-update/evaluation/c2c_dicts/geonames_locations_combined_country2code.pkl", help="path to a dictionary that converts country name to iso3166 alpha 2 country code")
    parser.add_argument("--quant-step", type=float, default=0.001, help="Step size for quantile plot. Must be a real number strictly between 0 and 1. Default: 0.001")
    parser.add_argument("--cell-size", type=float, default=None, help='Geocode resolver cell size')
    # parser.add_argument("--time-resolved", action="store_true", help="Time per-sample resolution time for resolved tweets")
    parser.add_argument("--task-id", type=str, default=None, help='SGE_TASK_ID metadata')
    return parser.parse_args()

def is_match(location, ground_truth):
    global name2code
    place_type, name, country_code = ground_truth
    # TODO: check if part of the ground truth is unavailable
    # mapped_country_code = name2code[location.country]
    # print(f'{location} ||| {ground_truth}')
    mapped_country_code = location.countrycode
    if mapped_country_code is not None:
        mapped_country_code = mapped_country_code.upper()
    else:
        mapped_country_code = name2code.get(location.country, 'und')
    # print(f'{mapped_country_code} <> {location.city} <> {location.city} <> {location.county} <> {location.state}')
    country_match = bool(mapped_country_code == country_code)
    if not country_match:
        logging.debug(f"{country_code} ||| {location.country} ||| {mapped_country_code}")
    admin_match = False
    city_match = False
    if place_type == 'city' and country_match and location.city == name:
        city_match = True
    if place_type == 'city':
        logging.debug(f"{city_match} ||| {name} ||| {location.city}")
    if place_type == 'admin' and country_match and (location.county == name or location.state == name):
        admin_match = True
    if place_type == 'admin':
        logging.debug(f"{admin_match} ||| {name} ||| {location.state} ||| {location.county}")

    # print(f'{city_match} >< {admin_match} >< {country_match}')
    return city_match, admin_match, country_match

def agg(args):
    # get all .pkltmp in 
    partials = [os.path.join(args.output_path, filename) for filename in os.listdir(args.output_path) if os.path.splitext(filename)[1] == '.partial']

    total = 0
    resolved = 0
    total_carmen_time = 0
    total_resolved_time = 0

    # different types of matches
    country_matches = 0
    city_matches = 0
    admin_matches = 0 # country or state match

    have_country_and_resolved = 0
    # twitter place type: either have "city" or "admin"
    have_city_and_resolved = 0
    have_admin_and_resolved = 0

    # in miles
    have_coords = 0
    avg_dist = 0.0
    dists = []
    # reference:
    # to_save = (
    #     total,
    #     resolved,
    #     total_carmen_time,
    #     total_resolved_time,
    #     country_matches,
    #     city_matches,
    #     admin_matches,
    #     have_country_and_resolved,
    #     have_city_and_resolved,
    #     have_admin_and_resolved,
    #     have_coords,
    #     avg_dist, # total dist / have_coords
    #     dists
    # )
    for filepath in partials:
        with open(filepath, 'rb') as f:
            obj = pickle.load(f)
        tot, res, tct, trt, com, cim, adm, hpl, hct, had, hco, avd, dst = obj
        total += tot
        resolved += res
        total_carmen_time += tct
        total_resolved_time += trt
        country_matches += com
        city_matches += cim
        admin_matches += adm
        have_country_and_resolved += hpl
        have_city_and_resolved += hct
        have_admin_and_resolved += had
        have_coords += hco
        avg_dist = (avg_dist * (have_coords-hco) + avd * hco) / have_coords
        dists += dst

    distQuantile = DistQuantile(dists, args.quant_step)

    # Compute metrics. use notation in the paper
    # WARNING: ASSUME ALL TWEETS ARE GEOTAGGED, i.e. total == num_geotagged
    coverage = resolved/total
    mr_country = country_matches/have_country_and_resolved
    mr_admin = admin_matches/have_admin_and_resolved if have_admin_and_resolved > 0 else -1
    mr_city = city_matches/have_city_and_resolved if have_city_and_resolved > 0 else -1
    d = avg_dist
    r_10 = distQuantile.get_quant(10)
    r_100 = distQuantile.get_quant(100)
    r_1000 = distQuantile.get_quant(1000)
    coords_ratio = have_coords/total
    per_resolved_sample_time = total_resolved_time/resolved if resolved > 0 else -1
    per_sample_time = total_carmen_time/total

    result_df = pd.DataFrame(
        np.array([
            coverage,
            mr_country,
            mr_admin,
            mr_city,
            d,
            r_10,
            r_100,
            r_1000,
            coords_ratio,
            per_resolved_sample_time,
            per_sample_time
            ]).reshape(1,-1), 
        columns=[
            'coverage',
            'mr_country',
            'mr_admin',
            'mr_city',
            'd',
            'r_10',
            'r_100',
            'r_1000',
            'coords_ratio',
            'tprs',
            'tps'
            ])
    result_df.to_csv(os.path.join(args.output_path, 'results.csv'), index=False)
    with open(os.path.join(args.output_path, 'distQuantile.pkl'), 'wb') as f:
        pickle.dump(distQuantile, f)

    print("Aggregation summary:")
    print(f'total = {total} (for coverage)')
    print(f'have_country_and_resolved = {have_country_and_resolved} (for mr_country)')
    print(f'have_admin_and_resolved = {have_admin_and_resolved} (for mr_admin)')
    print(f'have_city_and_resolved = {have_city_and_resolved} (for mr_city)')
    print(f'have_coords = {have_coords} (for d, r_xxx)')

    if args.store_dists:
        with open(args.dists_filename, "w") as f:
            for d in dists:
                print(d, file=f)

def entry_nonempty(entry):
    if entry is None:
        return False
    return len(entry) > 0

if __name__ == "__main__":
    args = parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    global name2code
    name2code = pickle.load(open(args.country_code_dict, "rb"))

    if args.aggregate:
        agg(args)
        exit(0)
    options_dict = None
    if args.cell_size:
        options_dict = {'geocode':{'cell_size':args.cell_size}}

    if not args.resolvers:
        resolver = carmen.get_resolver(options=options_dict)
    else:
        resolver = carmen.get_resolver(order=args.resolvers, options=options_dict)
    resolver.load_locations(location_file=args.location_file)

    # init statistics
    # macro stats
    total = 0
    resolved = 0
    total_carmen_time = 0
    total_resolved_time = 0

    # different types of matches
    country_matches = 0
    city_matches = 0
    admin_matches = 0 # country or state match

    have_country_and_resolved = 0
    # twitter place type: either have "city" or "admin"
    have_city_and_resolved = 0
    have_admin_and_resolved = 0

    # in miles
    have_coords = 0
    avg_dist = 0.0
    dists = []
    tweets_resolution_dists = [] # element is tuple (tweet, resolution, dist)

    for filename in args.input_file:
        reader = TweetReader(filename)
        for tweet in reader.read_tweets():
            total += 1

            start = time.time()
            resolution = resolver.resolve_tweet(tweet)
            end = time.time()
            total_carmen_time += end-start

            if resolution:
                resolved += 1
                location = resolution[1]
                total_resolved_time += end-start
                if tweet.get('place') is not None and tweet['place'].get('country') is not None: # related: mr_xx
                    truth_formatted = f"{tweet['place'].get('full_name', '')}, {tweet['place'].get('country', '')} ({tweet['place'].get('country_code', '')})"
                    ground_truth = tuple([tweet['place'].get(field, '') for field in ['place_type', 'name', 'country_code']])
                    
                    city_match, admin_match, country_match = is_match(location, ground_truth)

                    if tweet['place'].get('place_type', '') == 'city' and entry_nonempty(location.city): 
                        have_city_and_resolved += 1
                        city_matches += int(city_match)
                    if tweet['place'].get('place_type', '') == 'admin' and (entry_nonempty(location.county) or entry_nonempty(location.state)): 
                        have_admin_and_resolved += 1
                        admin_matches += int(admin_match)
                    if entry_nonempty(location.countrycode): 
                        have_country_and_resolved += 1
                        country_matches += int(country_match)

                # check distance, related: d, r_xx
                ground_truth_coords = (tweet.get('coordinates') or {}).get('coordinates')
                if ground_truth_coords is not None and location.latitude is not None and location.longitude is not None:
                    gt_point = Point(longitude=ground_truth_coords[0], latitude=ground_truth_coords[1])
                    candidate_point = Point(location.latitude, location.longitude)
                    distance = geopy_distance(gt_point, candidate_point).miles
                    s = avg_dist * have_coords
                    s += distance
                    have_coords += 1
                    avg_dist = s / have_coords
                    dists.append(distance)
                    tweets_resolution_dists.append((tweet, resolution, distance))
    
    if args.sort_dists_tweets:
        tweets_resolution_dists = sorted(tweets_resolution_dists, key=lambda x:-x[2])
        with open(args.sort_dists_tweets, "wb") as f:
            pickle.dump(tweets_resolution_dists, f)

    to_save = (
        total,
        resolved,
        total_carmen_time,
        total_resolved_time,
        country_matches,
        city_matches,
        admin_matches,
        have_country_and_resolved,
        have_city_and_resolved,
        have_admin_and_resolved,
        have_coords,
        avg_dist, # total dist / have_coords
        dists
    )
    name = args.task_id if args.task_id else os.path.basename(args.input_file[0])
    save_path = os.path.join(args.output_path, f'{name}.partial')
    with open(save_path, 'wb') as f:
        # just use first input's name so that filenames are distinct. they doesn't matter anyway
        pickle.dump(to_save, f)

