'''
Filter tweets base on criteria:
    - Each user have at most a single tweets that contain the user 
    profile location string (field "user_location")
    - Tweets without "user_location" are discarded and relevant
    statistics are collected

Author: Jack Zhang
'''

from littlebird import TweetReader, TweetWriter
from tqdm import tqdm
import argparse
import os
import pickle
# import logging

# logging.basicConfig(level=logging.INFO)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-file", "-i", nargs="*", type=str, default=[], help="Tweets in JSONlines format (.gz format allowed)")
    parser.add_argument("--output-dir", "-o", type=str, default='.', help="Output a pickle containing already counted entries represented in a Counter object")
    parser.add_argument("--ext", "-e", type=str, default=".gz", help="--aggregate need to know the extention of tweet jsonlines files")
    parser.add_argument("--aggregate", "-a", action="store_true")
    parser.add_argument("--keep-temp", action="store_true", help="Keep self-deduplicated middle version. Warning: takes 2x space.")
    return parser.parse_args()

def read_file_as_list(filepath):
    '''
    Each line in a file is a entry in the produced list
    '''
    ls = []
    with open(filepath, "r") as fin:
        for line in fin:
            ls.append(line.strip())
    return ls

def write_list_to_file(ls, filepath):
    '''
    Each entry in the list is written as a line in the output file
    '''
    with open(filepath, "w") as fout:
        for el in ls:
            print(el, file=fout)

def aggregate(args):
    agg_dir = os.path.join(args.output_dir, 'aggregate')
    agglog_dir = os.path.join(args.output_dir, 'log_aggregate')
    os.makedirs(agg_dir, exist_ok=True)
    os.makedirs(agglog_dir, exist_ok=True)
    mapped_users_path = os.path.join(agglog_dir, 'mapped_users.txt')
    mapped_files_path = os.path.join(agglog_dir, 'mapped_files.txt')
    mapped_users = set(read_file_as_list(mapped_users_path) if os.path.exists(mapped_users_path) else [])
    mapped_files = set(read_file_as_list(mapped_files_path) if os.path.exists(mapped_files_path) else [])

    for filename in tqdm(os.listdir(args.output_dir)[::-1], ncols=0, desc="Aggregation status"):
        # NOTE: traverse the dir in reverse order so that more recent tweets get preserved
        name, ext = os.path.splitext(filename)
        # print(f"A: {filename}")
        if ext != args.ext:
            continue # only process the relevant (.gz) files
        if filename in mapped_files:
            continue
        # print(f"B: {filename}")

        filtered = []
        inpath = os.path.join(args.output_dir, filename)
        reader = TweetReader(inpath)
        for tweet in reader.read_tweets():
            user_id = tweet.get('user_id_str')
            if (user_id is None) or (user_id in mapped_users):
                continue
            else:
                filtered.append(tweet)
                mapped_users.add(user_id)
        # print(f"C: {len(filtered)}")
        if len(filtered) > 0:
            outpath = os.path.join(agg_dir, filename) # same filename, different folder
            writer = TweetWriter(outpath)
            writer.write(filtered)
            
            deduplog_dir = os.path.join(args.output_dir, "log_deduplication")
            os.makedirs(deduplog_dir, exist_ok=True)
            logtxt = os.path.join(deduplog_dir, f"{name}.txt")
            with open(logtxt, "w") as fout:
                print(len(filtered), file=fout)
        mapped_files.add(filename)
        if not args.keep_temp:
            os.remove(inpath)

        # Update the state files after each .gz is processed to support frequent stopping and resuming
        write_list_to_file(list(mapped_files), mapped_files_path)
        write_list_to_file(list(mapped_users), mapped_users_path)


if __name__ == "__main__":
    args = parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    if args.aggregate:
        aggregate(args)
        exit(0)
    
    for filename in args.input_file:
        total = 0
        has_location = 0
        mapped_users = set()
        filtered_tweets = []
        reader = TweetReader(filename)
        for tweet in reader.read_tweets():
            total += 1
            loc = tweet.get('user_location')
            if (loc is None) or (len(loc) == 0):
                continue
            has_location += 1
            user_id = tweet.get('user_id_str')
            if (user_id is None) or (user_id in mapped_users):
                continue
            # tweet has location and is a new user
            mapped_users.add(user_id)
            filtered_tweets.append(tweet)

        # output
        input_dir, basename = os.path.split(filename)
        name, ext = os.path.splitext(basename)
        outpath = os.path.join(args.output_dir, f"{name}{ext}")
        loclog_dir = os.path.join(args.output_dir, "log_location")
        os.makedirs(loclog_dir, exist_ok=True)
        outtxt = os.path.join(loclog_dir, f"{name}.txt")
        writer = TweetWriter(outpath)
        writer.write(filtered_tweets)
        with open(outtxt, "w") as f:
            print(total, file=f)
            print(has_location, file=f)
            print(len(filtered_tweets), file=f)

        

    
