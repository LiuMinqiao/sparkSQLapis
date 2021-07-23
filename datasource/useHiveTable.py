#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
A simple example demonstrating Spark SQL Hive integration.
Run with:
  ./bin/spark-submit examples/src/main/python/sql/hive.py
"""
# $example on:spark_hive$
from os.path import abspath

from pyspark.sql import SparkSession
from pyspark.sql import Row

# $example off:spark_hive$


if __name__ == "__main__":
    # $example on:spark_hive$
    # warehouse_location points to the default location for managed databases and tables
    warehouse_location = abspath('spark-warehouse')
    #
    #  spark = SparkSession \
    #         .builder \
    #         .appName("Python Spark SQL Hive integration example") \
    #         .config("spark.sql.warehouse.dir", warehouse_location) \
    #         .enableHiveSupport() \
    #         .getOrCreate()
    #
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL Hive integration example") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sql("show databases").show()
    spark.sql("use liptv")
    spark.sql("show tables").show(500)
    spark.sql("use lmqredo")
    spark.sql("create table if not exists testpysparkl("
              "id int,"
              "age int)"
              "row format delimited fields terminated by ','")
    spark.sql("show tables like '*spark*'").show()

    spark.stop()

