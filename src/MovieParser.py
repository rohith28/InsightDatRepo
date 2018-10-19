# !/usr/bin/env python3.6
# -*- coding:utf-8 -*-

"""
Author: Rohith Kumar Uppala

"""
from pyspark import SparkContext
from pyspark.sql import SparkSession
from os import environ
from connectorHelper import connectorHelper
import DatabaseConnector
import datetime



class MovieParser:
    
    def __init__(self):
        
        # Spark Session creation
        self.spark = SparkSession \
            .builder \
            .master(environ["SPARK_HOST"]) \
            .appName("meta_info_processor") \
            .getOrCreate()
    
    def stop_spark(self):
        self.spark.stop()

        
    def ingest_movies_data(self,datapath):

        # Read csv file form S3 which contains raw movie information
        movieInfoDF = sqlContext.read.format('com.databricks.spark.csv')\
                    .options(header=True)\
                    .options(inferSchema=True)\
                    .load(datapath)
        
        movieInfoDF = movieInfoDF.drop("homepage","overview","status","tagline")
        movieInfoDF.registerTempTable("movieDetails")
        
        movieDetailsDF = sqlContext.sql("SELECT id as movie_id,title as movie_name,runtime,budget,revenue,original_language as language ,"+
                            "popularity,TO_DATE(CAST(UNIX_TIMESTAMP(release_date, 'MM/dd/yy') AS TIMESTAMP)) AS releasedate,"+
                            "vote_count as votes,vote_average as voteavg"+ 
                            "FROM movieDetails")
        
        movieDetailsDF.createOrReplaceTempView("view")
        

        DatabaseConnector.redshift_saver(spark, movieDetailsDF, tbname="movies", \
                                                tmpdir='tmp', savemode='append')

if __name__ == "__main__":
    

    parsingInstance = MovieParser()

    # Setup Driver for connection
    print("Using Redshift as database.")
    environ['PYSPARK_SUBMIT_ARGS'] = '--jars ./spark-redshift_2.10-3.0.0-preview1.jar \
                            --jars ./spark/jars/spark-avro_2.11-4.0.0.jar \
                            --jars ./spark/jars/RedshiftJDBC41-1.2.12.1017.jar pyspark-shell'

    # Setup python path for worker nodes
    environ['PYSPARK_PYTHON'] = '/home/ubuntu/anaconda3/bin/python'
    environ['PYSPARK_DRIVER_PYTHON'] = '/home/ubuntu/anaconda3/bin/jupyter'

    self.spark.sparkContext.addPyFile('/home/ubuntu/MOVIEINSIGHTS/src/connectorHelper.py')
    self.spark.sparkContext.addPyFile('/home/ubuntu/MOVIEINSIGHTS/src/databaseConnector.py')
    
    todayDate = datetime.datetime.today().strftime('%Y-%m-%d')
    year,month,day = todayDate.split('-')
    filename = 'moviesInfo' + ''.join((year, month, day)) + '.csv'
    path = 's3a://moviedatasetinsight/' + foldername + '/' + filename
    parsingInstance.ingest_movies_data(path)
    parsingInstance.stop_spark()

    