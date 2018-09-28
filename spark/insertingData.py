#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 26 09:28:47 2018

@author: rohith
"""
import boto3
import pandas as pd
from io import StringIO
import pymysql
import time

class dataProcessing():
    # Initialize
    def __init__(self,clusterURL = "local",appName = "AUTH"):
        
        self.userName = os.environ["user"]
        self.password = os.emviron["pwd"]
        self.ACCESS_KEY_ID = os.environ["ACCESS_KEY_ID"]
        self.ACCESS_SECRET_KEY = os.environ["ACCESS_SECRET_KEY"]
        self.BUCKET_NAME = 'moviedatasetinsight'
        self.srcFileName = 'tmdb_5000_movies.csv'
        self.region = 'us-east-1'
        
        # Debug statement
        print("Initialized")
    
    def connect_database(self):
        try:
            host="mysqlinstance.cjm8qag6rwgx.us-east-1.rds.amazonaws.com"
            port=3306
            dbname="moviesdatabase"
        
            conn = pymysql.connect(host, user=self.userName,port=port,
                           passwd=self.password, db=dbname)
            self.conn = conn
        except:
            print("cannot connect to db")
        
        return conn
    
    def create_cursor(self,conn):
        cur = conn.cursor()
        print("connection esatblished")
        return cur


    def read_file(self):
    
        client = boto3.client('s3', aws_access_key_id=self.ACCESS_KEY_ID,
                              aws_secret_access_key=self.ACCESS_SECRET_KEY,region_name=self.region)
        csv_obj = client.get_object(Bucket=self.BUCKET_NAME, Key=self.srcFileName)
        body = csv_obj['Body']
        csv_string = body.read().decode('utf-8')
        
        df = pd.read_csv(StringIO(csv_string))
        return df
    
    def dataIngestion(self,dataFrame,cursorObj):
        
        print("Data ingestion started ")
        start_time = time.time()
        
        '''
        columns = ['movie_name','runtime','budget','revenue','language','popularity','releasedate','votes','voteAvg']
        sqlDF = pd.DataFrame(columns)
        
        sqlDF['movie_name'] = dataFrame['title']
        sqlDF['runtime'] = dataFrame['runtime']
        sqlDF['budget'] = dataFrame['title']
        sqlDF['language'] = dataFrame['original_language']
        sqlDF['popularity'] = dataFrame['popularity']
        sqlDF['releasedate'] = dataFrame['release_date']
        sqlDF['votes'] = dataFrame['vote_count']
        sqlDF['voteAvg'] = dataFrame['vote_average']
        
        sqlDF.to_sql(con=self.conn, name='movies', if_exists='replace')
        
        
        '''
        cnt = 0
        for index, row in dataFrame.iterrows():
            
            insQuery = "INSERT INTO movies(movie_name,runtime,budget,revenue,language,popularity, releaseDate,votes,voteAvg) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            
            try:
                cursorObj.execute(insQuery,(row['title'],row['runtime'],row['budget'],row['revenue'],row['original_language'],row['popularity'],row['release_date'],row['vote_count'],row['vote_average']))
            except:
                print("problem inserting data")
                print(insQuery)
                print(row['title'])
                cnt+=1
        
        print("Time taken to execute %s"%(time.time() - start_time))                
        print("problem with %s",cnt)
    
    
if __name__ == "__main__":
    
    
    instance = dataProcessing()
    
    connection = instance.connect_database()
    cursor= instance.create_cursor(connection)
    csvData = instance.read_file()
    instance.dataIngestion(csvData,cursor)
    connection.commit()
    connection.close()
    print(type(csvData))
