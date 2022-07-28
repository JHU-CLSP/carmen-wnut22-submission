"""
Evaluate the accuracy of Carmen

Author: Jack Zhang
"""
from collections import Counter, OrderedDict
import carmen
from littlebird import TweetReader
import argparse
import logging
import pickle
import numpy as np
import pandas as pd
import time
import os

from dateutil import parser as date_parser
from geopy import Point
from geopy.distance import distance as geopy_distance

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def parse_args():
    parser = argparse.ArgumentParser()
    # program settings
    parser.add_argument("--aggregate", action="store_true")
    parser.add_argument("--verbose", "-v", action="store_true", help="Print details")
    # i/o
    parser.add_argument("--input-file", nargs="+", type=str, default="../data/dev_set.gz", help="Tweets in JSONlines format (.gz format allowed)")
    parser.add_argument("--output-path", type=str, default=".", help="Path for output csv")
    # carmen config
    parser.add_argument("--location-file", type=str, default="../carmen-python/carmen/data/locations.json")
    parser.add_argument("--resolvers", nargs="+", default=None, help="Limit Carmen to specific resolver")
    parser.add_argument("--cell-size", type=float, default=None, help='Geocode resolver cell size')
    parser.add_argument("--country-code-dict", type=str, default="/export/b10/jzhan237/carmen-update/evaluation/c2c_dicts/geonames_locations_combined_country2code.pkl", help="path to a dictionary that converts country name to iso3166 alpha 2 country code")
    # metainfo
    parser.add_argument("--task-id", type=str, default=None, help='SGE_TASK_ID metadata')
    return parser.parse_args()

def agg(args):
    # get all .pkltmp in 
    partials = [os.path.join(args.output_path, filename) for filename in os.listdir(args.output_path) if os.path.splitext(filename)[1] == '.partial']

    total = 0
    resolved = 0
    total_by_year = Counter()
    resolved_by_year = Counter()

    # to_save = (
    #     total,
    #     resolved,
    #     total_by_year,
    #     resolved_by_year
    # )
    for filepath in partials:
        with open(filepath, 'rb') as f:
            obj = pickle.load(f)
        tot, res, tot_y, res_y = obj
        total += tot
        resolved += res
        total_by_year += tot_y
        resolved_by_year += res_y
        
    def sort_dict(d):
        return OrderedDict(sorted(d.items()))
    total_by_year = sort_dict(total_by_year)
    resolved_by_year = sort_dict(resolved_by_year)

    with open(os.path.join(args.output_path, 'results.csv'), 'w') as f:   
        print(f'year,total,resolved', file=f)
        print(f'all,{total},{resolved}', file=f)
        for k in total_by_year.keys():
            year_total = total_by_year[k]
            year_resolved = resolved_by_year.get(k, 0)
            print(f'{k},{year_total},{year_resolved}', file=f)


def get_tweet_created_time(tweet):
    created_at = tweet.get('created_at')
    if not created_at:
        return None
    try:
        dt_obj = date_parser(created_at)
    except:
        return None
    return dt_obj

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
    total_by_year = Counter()
    resolved_by_year = Counter()

    # print(args.input_file)
    for filename in args.input_file:
        reader = TweetReader(filename)
        for tweet in reader.read_tweets():
            # print(f'1:{tweet}')
            dt_obj = get_tweet_created_time(tweet)
            # print(f'2:{tweet}')
            if dt_obj is None:
                year = -1
            else:
                year = dt_obj.year
            total += 1
            total_by_year[year] += 1
            resolution = resolver.resolve_tweet(tweet)
            if resolution:
                resolved += 1
                resolved_by_year[year] += 1

    to_save = (
        total,
        resolved,
        total_by_year,
        resolved_by_year
    )
    name = args.task_id if args.task_id else os.path.basename(args.input_file[0])
    save_path = os.path.join(args.output_path, f'{name}.partial')
    with open(save_path, 'wb') as f:
        # just use first input's name so that filenames are distinct. they doesn't matter anyway
        pickle.dump(to_save, f)

