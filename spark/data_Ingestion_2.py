#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 26 19:55:46 2018

@author: rohith
"""

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
import json
import sys

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
    
    def dataIngestionGenres(self,dataFrame,cursorObj):
        
        print("Data ingestion started ")
        start_time = time.time()
        
        cnt = 0
        for index, row in dataFrame.iterrows():
            jsonData = row[1]
            data = json.loads(jsonData)
            size = len(data)
            checkQuery = "SELECT `movie_id`,movie_name FROM `movies` WHERE `movie_name` =%s"
                
            try:
                #print(row['title'])
                cursorObj.execute(checkQuery,(row['title']))
                
            except pymysql.err.ProgrammingError as err:
                print(err)
            except:
                print("Problem in query",sys.exc_info()[0])
            res = cursorObj.fetchone()
            #print(cursorObj.rowcount)
            
            
            if cursorObj.rowcount > 0 :
                
                movie_id = int(res[0])
                for i in range(0,size):
                    insQuery = "INSERT INTO genres(movie_id,`genreName`) VALUES (%s,%s)"
                    try:
                        cursorObj.execute(insQuery,(movie_id,data[i]['name']))
                        #print(res[0])
                        #print(type(movie_id))
                        #print(type(data[i]['name']))
                    except pymysql.err.ProgrammingError as err:
                        print(err)
                        cnt+=1
                        print(movie_id)
                        print(res[1])
            
                    except pymysql.err.InternalError as err:
                        print(err)
                        cnt+=1
                        print(movie_id)
                        print(res[1])
            
                    except:
                        print(res[1])
            
                        print("problem inserting data",sys.exc_info()[0])
                        cnt+=1
                        
        print("Time taken to execute %s"%(time.time() - start_time))                
        print("problem with %s",cnt)
    
    
if __name__ == "__main__":
    
    
    instance = dataProcessing()
    
    connection = instance.connect_database()
    cursor= instance.create_cursor(connection)
    csvData = instance.read_file()
    instance.dataIngestionGenres(csvData,cursor)
    connection.commit()
    connection.close()
    print(type(csvData))
