from littlebird import TweetReader
import argparse
import logging
import pickle
import numpy as np
import pandas as pd
import time
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--aggregate", action="store_true")
    parser.add_argument("--input-file", nargs="+", type=str, default="../data/dev_set.gz", help="Tweets in JSONlines format (.gz format allowed)")
    parser.add_argument("--output-path", type=str, default=".", help="Path for output csv")
    parser.add_argument("--task-id", type=str, default=None, help='SGE_TASK_ID metadata')
    return parser.parse_args()

def agg(args):
    # get all .partial in 
    partials = [os.path.join(args.output_path, filename) for filename in os.listdir(args.output_path) if os.path.splitext(filename)[1] == '.partial']

    total = 0
    have_user_location = 0

    # to_save = (
    #     total,
    #     have_user_location
    # )
    for filepath in partials:
        with open(filepath, 'rb') as f:
            obj = pickle.load(f)
        tot, hvul = obj
        total += tot
        have_user_location += hvul

    result_df = pd.DataFrame(
        np.array([
            total,
            have_user_location
            ]).reshape(1,-1), 
        columns=[
            'total',
            'have_user_location'
            ])
    result_df.to_csv(os.path.join(args.output_path, 'results.csv'), index=False)


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


if __name__ == "__main__":
    args = parse_args()

    if args.aggregate:
        agg(args)
        exit(0)

    # init statistics
    # macro stats
    total = 0
    have_user_location = 0

    for filename in args.input_file:
        reader = TweetReader(filename)
        for tweet in reader.read_tweets():
            total += 1
            loc_string = get_user_location_string_v1(tweet)
            if len(loc_string) > 0:
                have_user_location += 1

    to_save = (
        total,
        have_user_location
    )
    name = args.task_id if args.task_id else os.path.basename(args.input_file[0])
    save_path = os.path.join(args.output_path, f'{name}.partial')
    with open(save_path, 'wb') as f:
        # just use first input's name so that filenames are distinct. they doesn't matter anyway
        pickle.dump(to_save, f)

