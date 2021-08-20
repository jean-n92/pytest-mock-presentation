import argparse
import logging
from neon.functions import *

def main(): # pragma: no cover
    logging.disable()
    parser = argparse.ArgumentParser(
        description='Get a random cat fact or save few of them into a table.')
    parser.add_argument('--facts', '-f', type=int, default=1, help='how many facts should the app extract')
    parser.add_argument('--waiting', '-w', type=int, default=1, help='inefficiency time expressed in seconds')
    parser.add_argument('--save', '-s', type=bool, default=False, help='should the application save everything or not')
    args = parser.parse_args()
    data: dict = process_data(args.facts, args.waiting)
    if args.save:
        spark: SparkSession = establish_spark()
        group_and_save(spark, data)
    else:
        for entry in data:
            print(entry["fact"])

if __name__ == "__main__":
    main()