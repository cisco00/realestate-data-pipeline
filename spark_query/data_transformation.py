from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import numpy as np
import os
import pandas as pd


spark = SparkSession.builder.appName("Spark Data").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

path_directory = "/zillow_realestate_raw_data_storage"

for file in os.listdir(path_directory):
    file_path = os.path.join(path_directory, file)

    if os.path.isfile(file_path):
        labor = spark.read.csv(file_path, header=True, inferSchema=True)

        labor = labor.toPandas()
        print("loaded file successfully")
