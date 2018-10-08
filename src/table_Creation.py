y  # !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 25 16:50:19 2018

@author: rohith
"""

import pymysql
import time
import sys
from DatabaseConnector import DatabaseConnector


class movies():

    def create_cursor(self, conn):
        cur = conn.cursor()
        print("connection esatblished")
        return cur

    def create_tables(self):

        dbconnector = DatabaseConnector()
        connection = dbconnector.get_connection()
        curObj = connection.cursor()
        cnt =0
        print("Creating Tables ")
        start_time = time.time()
        try:
            # Movies table created
            sqlQuery = "CREATE TABLE IF NOT EXISTS movies (movie_id INT AUTO_INCREMENT PRIMARY KEY, " \
                       "movie_name VARCHAR(255)  NOT NULL, runtime INT,budget INT, revenue BIGINT, language VARCHAR(255), " \
                       "popularity FLOAT,releasedate DATE, votes INT, voteAvg INT)";
            curObj.execute(sqlQuery)
            print("Movies table created")

            # Actor tables created
            actorQuery = "CREATE TABLE actors (actorId INT AUTO_INCREMENT PRIMARY KEY, " \
                         "movie_id int NOT NULL, actorName VARCHAR(255),characterName VARCHAR(255), " \
                         "gender INT, FOREIGN KEY(movie_id) REFERENCES movies(movie_id));"
            curObj.execute(actorQuery)
            print("Actors table created")

            # Generes tables created
            genresQuery = "CREATE TABLE genres (genreid INT AUTO_INCREMENT PRIMARY KEY, " \
                          "movie_id int NOT NULL, genreName VARCHAR(255), FOREIGN KEY(movie_id) " \
                          "REFERENCES movies(movie_id));"
            curObj.execute(genresQuery)
            print("Genres table created")

            # Crew tables created
            crewQuery = "CREATE TABLE crew (crewId INT AUTO_INCREMENT PRIMARY KEY, " \
                        "movie_id int NOT NULL, crewName VARCHAR(255),dept VARCHAR(255), " \
                        "FOREIGN KEY(movie_id) REFERENCES movies(movie_id));"
            curObj.execute(crewQuery)
            print("Crew table created")

            # Production tables created
            productionQuery = "CREATE TABLE production (pId INT AUTO_INCREMENT PRIMARY KEY, " \
                              "movie_id int NOT NULL, pName VARCHAR(255), " \
                              "FOREIGN KEY(movie_id) REFERENCES movies(movie_id));"
            curObj.execute(productionQuery)
            print("Production table created")

        except pymysql.err.ProgrammingError as err:
            print(err)
        except:
            print("Error in the creating tables")
            print("Unexpected error:", sys.exc_info()[0])
        curObj.close()
        connection.commit()
        dbConnector.close_connection()
        print("Time taken to execute %s" % (time.time() - start_time))
        print("problem with %s", cnt)


if __name__ == "__main__":
    movieInstance = movies()
    movieInstance.create_tables()