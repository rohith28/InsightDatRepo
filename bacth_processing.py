# -*- coding: utf-8 -*-

import pymysql
import csv
import json
import pymysql.cursors
import datetime
class spark():
    
    def connectDatabase(self,user,password):
        
        host="mysqlinstance.cjm8qag6rwgx.us-east-1.rds.amazonaws.com"
        port=3306
        dbname="moviebuff"
        user="mydb"
        password='9542582841'

        conn = pymysql.connect(host, user=user,port=port,
                           passwd=password, db=dbname)
        return conn
    
    def createTable(self,conn):
        
        cursorObj = conn.cursor()
        sqlQuery = "CREATE TABLE movies (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), time INT, releaseDate DATE, voteCount INT,voteAvg Int, genres VARCHAR(255), budget INT, revenue INT) "
        
        cursorObj.execute(sqlQuery)
    
    def csv_to_database(self,csv_file,conn):
        with open('tmdb_5000_movies-1.csv') as csv_file:
            csv_reader = csv.reader(csv_file,delimiter= ',')
            line_count = 0
            namedict = {}
            cursorObj = conn.cursor()
            for row in csv_reader:
                
                if line_count == 0:    
                    line_count+=1
                else:
                    jsonData = row[1]
                    data = json.loads(jsonData)
                    size = len(data)
                    name = set()
                    for i in range(0,size):
                        name.add(data[i]['name'])
                        namedict[row[3]] = name
                    date_str = row[11]
                    print(type(date_str))
                    print(date_str)
                    format_str = '%m/%d/%y'
                    datetime_obj = datetime.datetime.strptime(date_str, format_str)
                    dateStr = datetime_obj.strftime('%Y-%m-%d')
                        
                    tempGenreStr = ", ".join(name)
                    
                    try:
                        sqlInsert = "INSERT INTO movies (`name`,time,releaseDate, voteCount, voteAvg,`genres`, budget,revenue) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
                    #values = [(row[6],row[13],dateStr, row[19],row[18],tempGenreStr, row[0],row[12] )]
                        cursorObj.execute(sqlInsert,(row[6],row[13],dateStr, row[19],row[18],tempGenreStr, row[0],row[12] ))
                    except:
                        pass
                    conn.commit()
                        
            
            
            
            
if __name__ == '__main__':
    
    user="mydb"
    password="9542582841"
    sparkInstace=  spark()
    conn = sparkInstace.connectDatabase(user,password)
    sparkInstace.createTable(conn)
    sparkInstace.csv_to_database('tmdb_5000_movies-1.csv',conn)
    