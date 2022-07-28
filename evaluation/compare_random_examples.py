import argparse
import random
from littlebird import TweetReader, TweetWriter
import carmen
import os

def parse_args():
    parser = argparse.ArgumentParser()

    # basic
    parser.add_argument('--data_dir', type=str, required=True)
    parser.add_argument('--num', type=int, required=True)
    parser.add_argument('--output_dir', type=str, required=True)
    parser.add_argument('--pos', type=str, choices=['geo-c', 'geo-o'], required=True)
    parser.add_argument('--neg', type=str, choices=['default', 'geo-o'], required=True)
    parser.add_argument('--ext', type=str, default='.gz', choices=['.gz', '.json', '.jsonl'])

    parser.add_argument('--strong', action='store_true', help='only consider pos got non-provisional, neg did not get result')
    parser.add_argument('--sample_many_in_one_file', action='store_true')
    # carmen stuff
    parser.add_argument("--resolvers", nargs="+", default=None, help="Limit Carmen to specific resolver")

    # misc
    parser.add_argument('--seed', type=int, default=42)

    args = parser.parse_args()
    assert args.pos != args.neg
    return args


def sample_filepath_from_dir(dir, ext):
    files = [os.path.join(dir, filename) for filename in os.listdir(dir) if os.path.splitext(filename)[1] == ext]
    return random.choice(files)

def load_different_resolvers(args):
    LOC_DEFAULT="/home/jzhan237/export/carmen-update/carmen-python/carmen/data/locations.json"
    LOC_GEONAMES_COMBINED="/home/jzhan237/export/carmen-update/carmen-python/carmen/data/geonames_locations_combined.json"
    LOC_GEONAMES_ONLY="/home/jzhan237/export/carmen-update/carmen-python/carmen/data/geonames_locations_only.json"
    loc_mapping = {
        'default': LOC_DEFAULT,
        'geo-o': LOC_GEONAMES_ONLY,
        'geo-c': LOC_GEONAMES_COMBINED
    }
    pos_resolver = carmen.get_resolver(order=args.resolvers)
    neg_resolver = carmen.get_resolver(order=args.resolvers)
    pos_resolver.load_locations(location_file=loc_mapping[args.pos])
    neg_resolver.load_locations(location_file=loc_mapping[args.neg])

    return pos_resolver, neg_resolver

def resolve_non_provisional(tweet, resolver):
    '''get rid of provisional resolutions'''
    resolution = resolver.resolve_tweet(tweet)
    if resolution is not None:
        if resolution[0] == True: # True == is provisional
            resolution = None
    return resolution

def resolve_pos_neg_strong(tweet, pos_resolver, neg_resolver):
    pos_result = resolve_non_provisional(tweet, pos_resolver)
    neg_result = neg_resolver.resolve_tweet(tweet)
    if pos_result is not None and neg_result is None:
        # pos got non-provisional and neg can't even get provisional
        # success
        return (True, pos_result)
    else:
        return (False, None)

def resolve_pos_neg(tweet, pos_resolver, neg_resolver):
    pos_result = pos_resolver.resolve_tweet(tweet)
    neg_result = neg_resolver.resolve_tweet(tweet)
    if (pos_result is not None and neg_result is None) or \
        (pos_result is not None and neg_result is not None and not pos_result[0] and neg_result[0]):
        # pos got result, neg did not get result
        # pos got non-provisional, neg got provisional (not pos_result[0] means non-provisional)
        # success
        return (True, pos_result)
    else:
        return (False, None)

def get_tweet_id(tweet):
    if tweet.get('id_str') is not None:
        return tweet['id_str']
    elif tweet.get('id') is not None:
        return str(tweet['id'])
    else:
        return '-1'

def main(args):
    count = 0
    file_count = 0
    pos_r, neg_r = load_different_resolvers(args)
    collected_tweets = []
    locations_with_id = []
    while len(collected_tweets) < args.num:
        file_count += 1
        if file_count % 100 == 0:
            print(f'went through {file_count} files')
        reader = TweetReader(sample_filepath_from_dir(args.data_dir, args.ext))
        for tweet in reader.read_tweets():
            count += 1
            if count % 10000 == 0:
                print(f'went through {count} tweets')
            flag, location = resolve_pos_neg_strong(tweet, pos_r, neg_r) if args.strong else resolve_pos_neg(tweet, pos_r, neg_r)
            if flag:
                collected_tweets.append(tweet)
                locations_with_id.append((get_tweet_id(tweet), location))
                print(f'obtained {len(collected_tweets)}-th tweet, went through: {count} tweets, {file_count} files')
                if not args.sample_many_in_one_file:
                    break

    os.makedirs(args.output_dir, exist_ok=True)
    with open(os.path.join(args.output_dir, 'id_locations.txt'), 'w') as f:
        for id, location in locations_with_id:
            print(f'{id}\t{repr(location)}', file=f)
    writer = TweetWriter(os.path.join(args.output_dir, f'tweets{args.ext}'))
    writer.write(collected_tweets)

if __name__ == '__main__':
    args = parse_args()
    random.seed(args.seed)
    main(args)