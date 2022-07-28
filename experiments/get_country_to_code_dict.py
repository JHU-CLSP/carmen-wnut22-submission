'''
Get country <-> country code dict from a carmen location database

Author: Jack Zhang
'''

import jsonlines
import argparse
import os
import pickle

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-file", "-i", nargs="+", type=str, help="Path to a carmen location database")
    parser.add_argument("--output-dir", "-o", type=str, default='.')
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    for filename in args.input_file:
        country2code = {}
        with jsonlines.open(filename) as reader:
            for line in reader:
                country = line.get('country')
                countrycode = line.get('countrycode')
                if country is not None and countrycode is not None and \
                    len(country) > 0 and len(countrycode) > 0:
                    country2code[country] = countrycode
        
        input_dir, basename = os.path.split(filename)
        name, ext = os.path.splitext(basename)
        outpath = os.path.join(args.output_dir, f"{name}_country2code.pkl")
        pickle.dump(country2code, open(outpath, "wb"))

        

    
