'''
Collect Language statistics from a set of tweets

Author: Jack Zhang
'''

from littlebird import TweetReader
import argparse
from collections import Counter
import pickle


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-file", nargs="*", type=str, default=None, help="Tweets in JSONlines format (.gz format allowed)")
    parser.add_argument("--input-pkl", nargs="+", type=str, default=None, help="Input a pickle containing already counted entries represented in a Counter object. If there are multiple inputs, values are added together")
    parser.add_argument("--output-pkl", type=str, default='langstats.pkl', help="Output a pickle containing already counted entries represented in a Counter object")
    parser.add_argument("--output-file", type=str, default=None, help="Path to output text file")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()

    counter = Counter()
    if args.input_pkl is not None:
        for pkl in args.input_pkl:
            with open(pkl, "rb") as fin:
                c = pickle.load(fin)
                counter += c
    if args.input_file is not None:
        for filename in args.input_file:
            reader = TweetReader(filename)
            for tweet in reader.read_tweets():
                counter[tweet['lang']] += 1
    
    with open(args.output_pkl, "wb") as fout:
        pickle.dump(counter, fout)
    
    if args.output_file is not None:
        with open(args.output_file, "w") as f:
            for k,v in counter.most_common():
                print(f"{k}\t{v}", file=f)
    
    
