import sys

import urllib3

from neon.utils.functions import logger, parser, process_data
from neon.utils.sparkutils import establish_spark


def main():  # pragma: no cover
    urllib3.disable_warnings()
    args = parser(sys.argv[1:])
    data: dict = process_data(args.facts, args.waiting)
    if args.save:
        spark = establish_spark()
        (spark, data)
    else:
        for entry in data:
            logger().info(entry["fact"])


if __name__ == "__main__":
    main()
