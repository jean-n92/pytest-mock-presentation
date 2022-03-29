import logging
from typing import List

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as f


def establish_spark(warehouse: str):
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
             .config("spark.sql.warehouse.dir", warehouse)
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
        f"Grouping cat facts list, {len(facts)} elements")
    sc = SparkContext.getOrCreate()
    logging.debug("Spark context retrieved")
    rdd = sc.parallelize(facts)
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
        logging.error(f"Couldn't save dataframe \n {e}")
