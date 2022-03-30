"""
Simple showcase application for the mock presentation.
"""
import argparse
import logging
import random
import time
from distutils.log import debug, info
from typing import List, Optional, Union

import requests
from requests.exceptions import ConnectionError


def fake_request(waiting: Optional[int] = None) -> None:
    """
    Mock method that simulates an API request. What it does is just
    putting the application to sleep for few seconds to simulate
    the buffering time of a big payload. The time that needs to
    pass can be passed as optional input.
    """
    timer = waiting if waiting else random.choice(range(1, 10))
    t0 = time.perf_counter()
    time.sleep(timer)
    t1 = time.perf_counter() - t0
    logging.debug("Time elapsed: %.1f seconds", t1)
    return None


def retrieve_data(waiting: Optional[int] = None,
                  url: str = "https://catfact.ninja/fact") -> Union[dict, None]:
    """
    Function that retrieves a standard JSON content from a public API.
    If no custom url is specified, it will retrieve a certain number of
    custom cat facts. To simulate a slow connection or a very heavy
    data, the fake_request method had been embedded.
    """
    logging.debug(f"Now arranging API call to {url}")
    fake_request(waiting) if waiting >= 0 else None
    logging.debug("Starting call now...")
    try:
        data = requests.get(url=url).json()
        return data
    except ConnectionError:
        logging.error("Could not connect to remote host")
        return None


def process_data(usernumber: int = 1,
                 waiting: int = 0) -> List[dict]:
    """
    Function that puts a certain number of requests together.
    It serves a processing mechanism, and it is build upon the previous ones.
    It allows the user to specify a certain waiting time, along with a certain
    number of requests or output style.
    """
    req_collection = []
    for n in range(0, usernumber):
        data = retrieve_data(waiting)
        req_collection.append(data)
        logging.debug(
            f"Load {n + 1} of {usernumber} is done")
    return req_collection


def parser(args: List) -> argparse.Namespace:
    """
    Simple parser for command line usage
    """
    parser = argparse.ArgumentParser(
        description="Get a random cat fact or save few of them into a table.")
    parser.add_argument("--facts",
                        "-f",
                        type=int,
                        default=1,
                        help="how many facts should the app extract")
    parser.add_argument("--waiting",
                        "-w",
                        type=int,
                        default=1,
                        help="inefficiency time expressed in seconds")
    parser.add_argument("--save",
                        "-s",
                        type=bool,
                        default=False,
                        help="should the application save everything or not")
    return parser.parse_args(args)

# TODO check this one out with the proper documentation


def logger() -> logging.Logger:  # pragma: no cover
    """
    Standard logging functionalities. Provides both a file logger and a stream logger, in order to [...]
    """
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s", level=logging.DEBUG)
    logger = logging.getLogger("console")
    if not logger.hasHandlers(logging.FileHandler):
        logger.addHandler(logging.FileHandler)
        logger.setLevel(debug)
        logger.addHandler(logging.StreamHandler)
    if not logger.hasHandler(logging.StreamHandler):
        logger.setLevel(info)
    return logger
