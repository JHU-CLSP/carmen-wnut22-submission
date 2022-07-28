"""
Store resolved locations in a counter object

Author: Jack Zhang
"""
from collections import Counter, OrderedDict, defaultdict
from littlebird import TweetReader
import argparse
import logging
import pickle
import numpy as np
import pandas as pd
import time
import os

from dateutil import parser as date_parser

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
    # metainfo
    parser.add_argument("--task-id", type=str, default=None, help='SGE_TASK_ID metadata')
    return parser.parse_args()

def agg(args):
    # get all .pkltmp in 
    partials = [os.path.join(args.output_path, filename) for filename in os.listdir(args.output_path) if os.path.splitext(filename)[1] == '.partial']

    total_by_year = Counter()
    has_place_by_year = Counter()
    has_coordinates_by_year = Counter()
    has_strict_coords_by_year = Counter()
    has_profile_location_by_year = Counter()
    place_type_counts_by_year = {}
    lang_counts_by_year = {}

    # to_save = (
    #     total_by_year,
    #     has_place_by_year,
    #     has_coordinates_by_year,
    #     has_strict_coords_by_year,
    #     has_profile_location_by_year,
    #     place_type_counts_by_year,
    #     lang_counts_by_year
    # )
    for filepath in partials:
        with open(filepath, 'rb') as f:
            obj = pickle.load(f)
        t, hp, hc, hsc, hpl, ptc, lc = obj
        total_by_year += t
        has_place_by_year += hp
        has_coordinates_by_year += hc
        has_strict_coords_by_year += hsc
        has_profile_location_by_year += hpl
        place_type_counts_by_year = combine_defaultdict_counters(place_type_counts_by_year, ptc)
        lang_counts_by_year = combine_defaultdict_counters(lang_counts_by_year, lc)
        
    def sort_dict(d):
        return OrderedDict(sorted(d.items()))
    total_by_year = sort_dict(total_by_year)
    has_place_by_year = sort_dict(has_place_by_year)
    has_coordinates_by_year = sort_dict(has_coordinates_by_year)
    has_strict_coords_by_year = sort_dict(has_strict_coords_by_year)
    has_profile_location_by_year = sort_dict(has_profile_location_by_year)
    place_type_counts_by_year = sort_dict(place_type_counts_by_year)
    lang_counts_by_year = sort_dict(lang_counts_by_year)

    with open(os.path.join(args.output_path, 'results.csv'), 'w') as f:   
        print(f'year,total,has_place,has_coords,has_strict_coords,has_profile_location', file=f)
        for year in total_by_year.keys():
            print(f'{year},{total_by_year[year]},{has_place_by_year.get(year,0)},{has_coordinates_by_year.get(year,0)},{has_strict_coords_by_year.get(year,0)},{has_profile_location_by_year.get(year,0)}', file=f)

    with open(os.path.join(args.output_path, 'place_type_counts_by_year.pkl'), 'wb') as f:
        pickle.dump(place_type_counts_by_year, f)
    print('place types:')
    for k,v in place_type_counts_by_year.items():
        print(f'year {k}')
        print(v)  
    
    with open(os.path.join(args.output_path, 'lang_counts_by_year.pkl'), 'wb') as f:
        pickle.dump(lang_counts_by_year, f)
    print('language:')
    for k,v in lang_counts_by_year.items():
        print(f'year {k}')
        print(v)  


def get_tweet_created_time(tweet):
    created_at = tweet.get('created_at')
    if not created_at:
        return None
    try:
        dt_obj = date_parser.parse(created_at)
    except:
        return None
    return dt_obj

def get_tweet_year(tweet):
    dt_obj = get_tweet_created_time(tweet)
    year = -1 if dt_obj is None else dt_obj.year
    return year

def get_tweet_lang(tweet):
    lang = tweet.get('lang')
    lang = lang if lang is not None else 'unk'
    return lang

def safe_get(dic, key):
    temp = dic.get(key)
    return {} if temp is None else temp

def get_user_location_string_v1(tweet):
    '''works only for api v1! in v2, need to hydrate author_id to get user object'''
    # tweet -> user -> location
    user_location = safe_get(tweet, 'user').get('location')
    if user_location is None: user_location = ''
    if len(user_location) > 0:
        return user_location
    # try a different place
    # tweet -> user_location
    user_location = tweet.get('user_location')
    if user_location is None: user_location = ''
    return user_location

def have_coordinates(tweet, count_bounding_box=False):
    '''adapted from carmen geocode resolver'''
    data = tweet.get('data')
    if data is None:
        # API v1
        v2 = False
        tweet_coordinates = (tweet.get('coordinates') or {}).get('coordinates')
    else:
        # API v2
        v2 = True
        geo = data.get('geo') or {}
        tweet_coordinates = (geo.get('coordinates') or {}).get('coordinates')

    if tweet_coordinates is not None:
        return True
    elif count_bounding_box:
        if v2:
            # Works in v2
            # Enhancement (Jack 09/15/21): another way to get coordinates is from 
            #       includes->places->[0]->geo->bbox
            # the bbox is a list of four coordinates. 
            # Avg 0 and 2 to get 1st coord, and avg 1 & 3 to get the 2nd 
            places = tweet.get('includes', {}).get('places', None)
            if not places:
                return False
            place = places[0]
            bbox = place.get('geo', {}).get('bbox')
            return (bbox is not None)
        else:
            # v1:
            #       place->bounding_box->coordinates->[0]->a list of lists of len 2 (long, lat)
            coords = safe_get(safe_get(tweet, 'place'), 'bounding_box').get('coordinates')
            return (coords is not None)
    else:
        return False


def combine_defaultdict_counters(x,y):
    return {k:(x.get(k,Counter()) + y.get(k,Counter())) for k in (set(x.keys()).union(set(y.keys())))}

if __name__ == "__main__":
    args = parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    if args.aggregate:
        agg(args)
        exit(0)

    # init statistics
    total_by_year = Counter()
    has_place_by_year = Counter()
    has_coordinates_by_year = Counter()
    has_strict_coords_by_year = Counter()
    has_profile_location_by_year = Counter()
    place_type_counts_by_year = defaultdict(Counter)
    lang_counts_by_year = defaultdict(Counter)

    # print(args.input_file)
    for filename in args.input_file:
        reader = TweetReader(filename)
        for tweet in reader.read_tweets():
            year = get_tweet_year(tweet)
            loc_string = get_user_location_string_v1(tweet)
            place_obj = tweet.get('place') # might be None: 'place' key does not exist or value is None
            
            total_by_year[year] += 1
            if place_obj is not None: 
                has_place_by_year[year] += 1
                place_type = place_obj.get('place_type')
                place_type = place_type if place_type is not None else 'N/A'
                place_type_counts_by_year[year][place_type] += 1
            if have_coordinates(tweet, count_bounding_box=True): has_coordinates_by_year[year] += 1
            if have_coordinates(tweet, count_bounding_box=False): has_strict_coords_by_year[year] += 1
            if len(loc_string) > 0: has_profile_location_by_year[year] += 1
            lang_counts_by_year[year][get_tweet_lang(tweet)] += 1
            
    to_save = (
        total_by_year,
        has_place_by_year,
        has_coordinates_by_year,
        has_strict_coords_by_year,
        has_profile_location_by_year,
        place_type_counts_by_year,
        lang_counts_by_year
    )
    name = args.task_id if args.task_id else os.path.basename(args.input_file[0])
    save_path = os.path.join(args.output_path, f'{name}.partial')
    with open(save_path, 'wb') as f:
        # just use first input's name so that filenames are distinct. they doesn't matter anyway
        pickle.dump(to_save, f)

