#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 25 16:50:19 2018

@author: rohith
"""

import pymysql
import time
import sys
class movies():
    # Initialize
    def __init__(self,clusterURL = "local",appName = "AUTH"):
        
        self.userName = "mydb"
        self.password = "9542582841"
        
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

    def create_tables(self,curObj):
        
        
        print("Data ingestion started ")
        start_time = time.time()
        cnt=0
        try:
            # Movies table created 
            sqlQuery = "CREATE TABLE IF NOT EXISTS movies (movie_id INT AUTO_INCREMENT PRIMARY KEY, movie_name VARCHAR(255)  NOT NULL, runtime INT,budget INT, revenue BIGINT, language VARCHAR(255), popularity FLOAT,releasedate DATE, votes INT, voteAvg INT)";
            curObj.execute(sqlQuery)
            print("Movies table created")
            
            
            # Actor tables created
            actorQuery = "CREATE TABLE actors (actorId INT AUTO_INCREMENT PRIMARY KEY, movie_id int NOT NULL, actorName VARCHAR(255),characterName VARCHAR(255), gender INT, FOREIGN KEY(movie_id) REFERENCES movies(movie_id));"
            curObj.execute(actorQuery)
            print("Actors table created")
            
            # Generes tables created
            genresQuery = "CREATE TABLE genres (genreid INT AUTO_INCREMENT PRIMARY KEY, movie_id int NOT NULL, genreName VARCHAR(255), FOREIGN KEY(movie_id) REFERENCES movies(movie_id));"
            curObj.execute(genresQuery)
            print("Genres table created")
            
            # Crew tables created
            crewQuery = "CREATE TABLE crew (crewId INT AUTO_INCREMENT PRIMARY KEY, movie_id int NOT NULL, crewName VARCHAR(255),dept VARCHAR(255), FOREIGN KEY(movie_id) REFERENCES movies(movie_id));"
            curObj.execute(crewQuery)
            print("Crew table created")
            
            # Production tables created
            productionQuery = "CREATE TABLE production (pId INT AUTO_INCREMENT PRIMARY KEY, movie_id int NOT NULL, pName VARCHAR(255), FOREIGN KEY(movie_id) REFERENCES movies(movie_id));"
            curObj.execute(productionQuery)
            print("Production table created")
        
        except pymysql.err.ProgrammingError as err:
             print(err)
        except:
            print("Error in the creating tables")
            print("Unexpected error:", sys.exc_info()[0])
        
        print("Time taken to execute %s"%(time.time() - start_time))                
        print("problem with %s",cnt)

if __name__ == "__main__":
    
    movieInstance = movies()
    conn = movieInstance.connect_database()
    cur= movieInstance.create_cursor(conn)
    movieInstance.create_tables(cur)
    conn.commit()
    conn.close()
    
