# Description:    PySpark code to calculate the supply of drivers at agiven point of time

# Revision History
#
# Date          Author          Description
# ---------------------------------------------------------------------------------------------------------------
# 2018-08-27    piyush
# ---------------------------------------------------------------------------------------------------------------



# !/usr/bin/env python



from pyspark.sql.functions import *
from pyspark import SparkFiles
import json
from mailer import Message
from mailer import Mailer
import os
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
from datetime import datetime


"""##########################  NOTES   ##################################################################

Specify the file_output_location in where you want to store the output parquet files
The table named "tbl_recipe" will be created in the default database of your metastore.

########################################################################################################
 """

#   Set the initial parameters
dir_path = os.path.dirname(os.path.realpath(__file__))
json_file_output_location = dir_path+'/output_file_json'
parquet_file_output_location = dir_path+'/output_file_parquet'
districts_json_file ='districts.json'
drivers_log_file ='drivers.log'

"""The schema of the input drivers log file """

driverSchema = StructType([ StructField("timestamp", StringType(), True)\
                       ,StructField("version", StringType(), True) \
                       , StructField("a", StringType(), True) \
                       ,StructField("b", StringType(), True)\
                       ,StructField("channel", StringType(), True)\
                       ,StructField("host", StringType(), True) \
                       , StructField("i", StringType(), True) \
                       , StructField("level", StringType(), True) \
                       , StructField("lg", StringType(), True) \
                       , StructField("lt", StringType(), True) \
                        ,StructField("message", StringType(), True) \
                        , StructField("s", StringType(), True) \
                        , StructField("sp", StringType(), True) \
                        , StructField("st", StringType(), True) \
                        , StructField("ts", StringType(), True) \
                      , StructField("type", StringType(), True)])

"""The schema of the final output  file """

outputSchema = StructType([StructField("driver_id", StringType(), True) \
                              , StructField("driver_status", StringType(), True) \
                              , StructField("service_type", StringType(), True) \
                              , StructField("event_timestamp", StringType(), True) \
                              , StructField("district_id", StringType(), True) \
                              , StructField("district_name", StringType(), True) \
                              , StructField("city_id", StringType(), True) \
                              , StructField("city_name", StringType(), True)])


# Create the spark session.
spark = SparkSession.builder.enableHiveSupport() \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .appName("recipes_etl_load").getOrCreate()



"""The Email class is defined to send the Email in case of failures. Localhost is used to configure the task"""

class Email():

    def __init__(self, m):
        self.message = m

    def send_email(self):
        try:
            message = self.message
            message = Message(From="localhost", To="hellofresh@gmail.com")
            message.Subject = "An HTML Email"
            message.Html = """<p>Hello!<br>The pipeline has failed Fix this and rerun it again message</p>"""
            sender = Mailer('localhost')
            sender.send(message)

        except TestFailed as message:
            print(message)


"""Exception class is created to catch the exceptions occuring due to unwanted scenarios"""

class TestFailed(Exception):
    def __init__(self, m):
        self.message = m
    def __str__(self):
        return self.message



class ReadInputFiles:
    """This module takes two csv customer and sales as input and returns the report sales_per_customer"""
    def __init__(self, input_file):
        self.filename = input_file

    def read_source_data(self):
        print("Log Step 1: Reading Source Data")
        try:
            if "json" in self.filename:
                with open(self.filename, newline=''):
                    file_path = dir_path + "/" + self.filename
                    disticts_df = spark.read.json(SparkFiles.get(file_path))
                return (disticts_df)
            elif "log" in self.filename:
                list_final = []
                with open(self.filename) as f:
                    for line in f:
                        list_a = json.loads(line)
                        list_final.append(list_a)

                drivers_df = pd.DataFrame(list_final)
                drivers_df = spark.createDataFrame(drivers_df, schema=driverSchema)

                return(drivers_df)

        except TestFailed as message:
            print("Entered exception")
            print(message)
            email = Email(message)
            email.send_email()


if __name__ == "__main__":
    start_time = datetime.utcnow()
    print("Log - Job Started. %s" % (datetime.now().strftime('%m/%d/%Y %H:%M:%S')))

    try:
        """Read the districts json file"""

        files = ReadInputFiles(districts_json_file)
        disticts_df = files.read_source_data()
        disticts_df=disticts_df.select("type", explode("features").alias("features")).select("features.*").select("geometry.coordinates","geometry.type","properties.*")
        row = disticts_df.count()
        print('No of records read from disticts.json file are: '+str(row))

        """Read the drivers log  file"""

        files = ReadInputFiles(drivers_log_file)
        drivers_df = files.read_source_data()
        row = drivers_df.count()
        print('No of records read from drivers.log file are: '+str(row))

        districts_coordinates =list(disticts_df.select('coordinates','district_id','district_name','city_id','city_name').collect())
        driver_coordinates = np.array(drivers_df.select('lg','lt','i','s','st','ts').collect())


        sc = spark.sparkContext
        drivers_enriched_df = []


        """For every driver log check the ditricts dataset to find which districts the driver belongs to. If the 
           match is found append the other details from the districts dataset
        """
        for i in range(0,len(driver_coordinates)):
            for j in range(0,len(districts_coordinates)):
                coordinates = districts_coordinates[j][0][0]
                polygon = Polygon(coordinates)
                y,x = float(driver_coordinates[i][0]),float(driver_coordinates[i][1])
                point = Point(y,x)  # create point
                if point.within(polygon):  #check if the driver's coordinates lies within the polygon created by district
                    driver_update_df = [str(driver_coordinates[i][2]),str(driver_coordinates[i][3]),str(driver_coordinates[i][4]),
                                         str(driver_coordinates[i][5])\
                        ,str(districts_coordinates[j][1]),str(districts_coordinates[j][2]),str(districts_coordinates[j][3])
                                            ,str(districts_coordinates[j][4])]

                    drivers_enriched_df.append(driver_update_df)
                    break #break the loop as soon the match is found

        """Create the dataframe by first creating an rdd from the drivers_enriched_df """
        output_df = spark.createDataFrame(sc.parallelize(drivers_enriched_df), outputSchema)

        """Perform the transformations on the """
        output_df = output_df.withColumn("datetime_formatted", unix_timestamp("event_timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'").cast(TimestampType()))\
            .withColumn("date", to_date("datetime_formatted")) \
            .withColumn("hour", hour("datetime_formatted")) \
            .withColumn("5_min_interval", window("datetime_formatted", "5 minutes")) \
            .drop("event_timestamp", "datetime_formatted")

        """Register the dataframe as view to write a SQL query"""

        output_df.createOrReplaceTempView("drivers_info")

        """SQL query to find the final result"""

        drivers_info_sql = ("""SELECT date,
                                        hour
                                        ,5_min_interval
                                        ,district_id
                                        ,district_name
                                        ,service_type
                                        ,driver_status
                                        ,city_id
                                        ,city_name
                                        ,count(driver_id) as supply
                                FROM drivers_info 
                                GROUP BY 
                                    date, hour,5_min_interval,district_id,district_name,service_type,driver_status,city_id,city_name
                                ORDER BY 
                                    date,hour,5_min_interval """)

        """The results dataframe contains the final output"""
        results_df = spark.sql(drivers_info_sql)

        """Write the results to the disk in both json and parquet formats"""
        results_df.coalesce(2).write.format("json").mode("overwrite").option("header", "true").json(json_file_output_location)
        results_df.coalesce(2).write.format("parquet").mode("overwrite").option("header", "true").parquet(parquet_file_output_location)
        print(results_df.count())

    except TestFailed as message:
        print("Entered exception")
        print(message)
        email = Email(message)
        email.send_email()

    end_time = datetime.utcnow()
    duration = (end_time - start_time).total_seconds()
    print("Log - The duration of the job was %s seconds" % duration)
    print("Log - Job Completed timestamp:. %s" % (datetime.now().strftime('%m/%d/%Y %H:%M:%S')))

