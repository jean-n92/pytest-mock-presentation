import random
import time
import logging
import requests
import names
import pyspark

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from dataclasses import dataclass
from typing import List, Optional

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)


@dataclass
class CatFact:
    fact: str
    length: int
    author: str


def make_request(usertime: Optional[int] = None) -> None:
    """
    Mock method that simulates an API request. What it does is just
    putting the application to sleep for few seconds to simulate
    the buffering time of a big payload. The time that needs to
    pass can be passed as optional input.
    """
    timer = usertime if usertime else random.choice(range(1, 10))
    t0 = time.perf_counter()
    time.sleep(timer)
    t1 = time.perf_counter() - t0
    logging.debug("Time elapsed: %.1f seconds", t1)
    return None


def retrieve_data(usernumber: Optional[int] = None,
                  waiting: Optional[bool] = None) -> List[CatFact]:
    """
    Function that retrieves a standard JSON content from a public API.
    If no custom url is specified, it will retrieve a certain number of
    custom cat facts. To simulate a slow connection or a very heavy
    data, the make_request method had been embedded.
    """
    num_requests = usernumber if usernumber else random.choice(range(1, 0))
    req_collection = []
    req_url = "https://catfact.ninja/fact"
    for n in range(0, num_requests):
        logging.debug("Now arranging API call to %s" % req_url)
        make_request() if waiting else None
        logging.debug("Starting call now")
        data = requests.get(url=req_url).json()
        req_collection.append(
            CatFact(
                fact=data["fact"],
                length=data["length"],
                author=names.get_full_name()
            )
        )
        logging.debug(
            "Load {a} of {b} is done".format(a=n+1, b=num_requests))
    return req_collection


if __name__ == "__main__":
    data = retrieve_data(usernumber=2, waiting=False)
    print(data)
