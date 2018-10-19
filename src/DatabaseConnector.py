# !/usr/bin/env python3.6
# -*- coding:utf-8 -*-

"""
Load from and save files to Redshift with jdbc drivers
Author: Rohith Kumar Uppala
"""

from pyspark.sql import SparkSession, Row
from os import environ

def redshift_saver(spark, df, tbname, tmpdir, savemode='append'):
    """
    Save DataFrame to Redshift via JDBC redshift driver
    
    :param spark: SparkSession
    :param df: dataframe to be saved
    :param tbname: table name
    :param tmpdir: tmp S3 dir to store data
    :param savemode: error: report error if exists
                     overwrite: overwrite the table if exists
                     append: append the the table if exists
                     ignore: no updates if the table exists
    """
    df.createOrReplaceTempView("view")
    spark.sql('''SELECT * FROM view''') \
        .write.format("com.databricks.spark.redshift") \
        .option("url", environ["jdbc_accessible_host_redshift"]) \
        .option("dbtable", tbname) \
        .option("forward_spark_s3_credentials", True) \
        .option("tempdir", "s3n://moviedatasetinsight/%s" % tmpdir) \
        .mode(savemode) \
        .save()


def redshift_loader(spark, tbname, tmpdir):
    """
     Load table from Redshift and return DataFrame
    :param spark: SparkSession
    :param tbname: The table to be loaded
    :param tmpdir: tmp S3 dir to store data
    :return: DataFrame load from Redshift table
    """
    print("Loading files from Redshift table: %s" % tbname)
    resultDF = spark.read \
        .format("com.databricks.spark.redshift") \
        .option("url", environ["jdbc_accessible_host_redshift"]) \
        .option("forward_spark_s3_credentials", True) \
        .option("dbtable", tbname) \
        .option("tempdir", "s3n://moviedatasetinsight/%s" % tmpdir) \
        .load()
    return resultDF


if __name__ == "__main__":
    # Setup Driver for connection
    # Loading Redshift driver to ingest and read data
    environ['PYSPARK_SUBMIT_ARGS'] = '--jars ./jars/spark-redshift_2.11-3.0.0-preview1.jar \
                            --jars ./jars/spark-avro_2.11-4.0.0.jar \
                            --jars ./jars/RedshiftJDBC41-1.2.12.1017.jar \
                            --jars ./jars/minimal-json-0.9.5.jar pyspark-shell'

    # Setup python path for worker nodes
    environ['PYSPARK_PYTHON'] = '/home/ubuntu/anaconda3/bin/python'
    environ['PYSPARK_DRIVER_PYTHON'] = '/home/ubuntu/anaconda3/bin/jupyter'

    spark = SparkSession \
        .builder \
        .master(__credential__.spark_host) \
        .appName("meta_info_loador") \
        .getOrCreate()

    spark.stop()