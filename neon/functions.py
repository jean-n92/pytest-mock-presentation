import os
import random
import time
import logging
import requests

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from typing import List, Optional

LOGGER = logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)
PATH = os.path.abspath(os.path.dirname(__file__))


def make_request(waiting: Optional[int] = None) -> None:
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


def retrieve_data(waiting: Optional[int] = None) -> dict:
    """
    Function that retrieves a standard JSON content from a public API.
    If no custom url is specified, it will retrieve a certain number of
    custom cat facts. To simulate a slow connection or a very heavy
    data, the make_request method had been embedded.
    """
    req_url = "https://catfact.ninja/fact"
    logging.debug("Now arranging API call to %s" % req_url)
    make_request(waiting) if waiting >= 0 else None
    logging.debug("Starting call now")
    data = requests.get(url=req_url).json()
    return data


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
            "Load {a} of {b} is done".format(a=n+1, b=usernumber))
    return req_collection


def establish_spark():
    """
    Simple function to establish a Spark Session if needed.
    It has already in itself the configuration property that
    allows it to use the local repository as warehouse.
    """
    logging.getLogger("pyspark").setLevel(logging.ERROR)
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.debug("Now establishing the Spark Session")
    spark = (SparkSession
             .builder
             .appName("Cat facts")
             .config("spark.sql.warehouse.dir", PATH + "/local_warehouse")
             .enableHiveSupport()
             .getOrCreate())
    logging.debug("Spark session is established")
    return spark


def group_and_save(spark: SparkSession, facts: List[dict]):
    """
    Function that receives the JSON data, parses it into a Spark Dataframe
    and then appends it into my table random_cats_facts.
    """
    logging.debug(
        "Grouping cat facts list, {number} elements".format(number=len(facts)))
    sc = SparkContext.getOrCreate()
    logging.debug("Spark context retrieved")
    rdd = sc.parallelize(data)
    df = (spark.read.json(rdd)
          .withColumn("load_dts", f.current_timestamp()))
    logging.debug("Dataframe created")
    df = df.select(
        "fact",
        "length",
        "load_dts"
    )
    try:
        df.write.mode("append").saveAsTable("default.random_cats_facts")
        logging.debug("Table has been saved")
    except Exception as e:
        logging.error("Couldn't save dataframe \n %s" % e)


if __name__ == "__main__":
    data = process_data(usernumber=3, waiting=1)
    spark = establish_spark()
    group_and_save(spark, data)