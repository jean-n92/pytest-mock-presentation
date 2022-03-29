import logging
import sys
from typing import List

from neon.utils.functions import parser, process_data
from neon.utils.sparkutils import establish_spark, group_and_save


def main():  # pragma: no cover
    logging.disable()
    args = parser(sys.argv[1:])
    data: dict = process_data(args.facts, args.waiting)
    if args.save:
        spark = establish_spark()
        group_and_save(spark, data)
    else:
        for entry in data:
            print(entry["fact"])


if __name__ == "__main__":
    main()
