'''
Filter tweets to two files, one in English, the other for other languages

Author: Jack Zhang
'''

from littlebird import TweetReader, TweetWriter
import argparse
import os


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-files", nargs="+", type=str, help="Tweets in JSONlines format (.gz format allowed)")
    parser.add_argument("--output-dir", type=str, required=True)
    parser.add_argument("--language-code", type=str, default='en', help="Two letter language code")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    inside_dir = os.path.join(args.output_dir, 'inside_tweets')
    outside_dir = os.path.join(args.output_dir, 'outside_tweets')
    os.makedirs(inside_dir, exist_ok=True)
    os.makedirs(outside_dir, exist_ok=True)
    CODE = args.language_code

    total = 0
    has_lang = 0
    inside_count = 0
    outside_count = 0
    for filename in args.input_files:
        inside_tweets = []
        outside_tweets = []

        reader = TweetReader(filename)
        for tweet in reader.read_tweets():
            total += 1
            langid = tweet.get('lang')
            if langid is None:
                continue
            has_lang += 1
            if langid == CODE:
                inside_tweets.append(tweet)
                inside_count += 1
            else:
                outside_tweets.append(tweet)
                outside_count += 1

        # output
        _ , basename = os.path.split(filename)
        writer = TweetWriter(os.path.join(inside_dir, basename))
        writer.write(inside_tweets)
        writer = TweetWriter(os.path.join(outside_dir, basename))
        writer.write(outside_tweets)

    outtxt = os.path.join(args.output_dir, "metadata.txt")
    with open(outtxt, "w") as f:
        print(total, file=f)
        print(has_lang, file=f)
        print(inside_count, file=f)
        print(outside_count, file=f)

        

    
