import argparse
import os
import pandas as pd

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-file", "-i", nargs="+", type=str, help="list of input csv")
    parser.add_argument("--output-path", "-o", type=str, default="results.csv", help="Path for output csv")
    parser.add_argument("--no-format", action="store_true")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()

    df = pd.DataFrame()
    names = []
    for input_path in args.input_file:
        _, basename = os.path.split(input_path)
        name, _ = os.path.splitext(basename)
        names.append(name)
        df_t = pd.read_csv(input_path)
        df = df.append(df_t)
    df.index = names
    if not args.no_format:
        cols_to_perc = ['coverage', 'mr_country', 'mr_admin', 'mr_city', 'coords_ratio']
        df[cols_to_perc] = df[cols_to_perc].applymap(lambda x:'{:.2%}'.format(x))
        df[['d']] = df[['d']].applymap(lambda x:'{:.1f}'.format(x))
        f3_cols = ['r_10', 'r_100', 'r_1000']
        df[f3_cols] = df[f3_cols].applymap(lambda x:'{:.3f}'.format(x))
    df.to_csv(args.output_path)
