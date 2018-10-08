y  # !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 25 16:50:19 2018

@author: rohith
"""

import pymysql
import time
import sys
from connectorHelper import connectorHelper


class tableCreator():

    def create_cursor(self, conn):
        cur = conn.cursor()
        print("connection esatblished")
        return cur

    def create_tables(self):

        dbconnector = connectorHelper()
        connection = dbconnector.get_connection()
        curObj = connection.cursor()
        cnt =0
        print("Creating Tables ")
        start_time = time.time()
        try:

            dropQuery = "DROP TABLE IF EXISTS movies"
            curObj.execute(dropQuery)
            
            # Movies table creation
            sqlQuery = """ CREATE TABLE movies(
                movie_id INT PRIMARY KEY,
                movie_name text,
                runtime INT,
                budget INT,
                revenue BIGINT,
                language text,
                popularity NUMERIC(5,2),
                releasedate DATE,
                votes INT,
                voteAvg INT   
                )"""
            
            curObj.execute(sqlQuery)
            print("Movies table created")

            # Actor table actor movie relational table
            actorQuery = """ CREATE TABLE actors(actorId INT PRIMARY KEY,
                    actorName text,
                    gender INT
            )
            """
            curObj.execute(actorQuery)

            actorMovieQuery = """CREATE TABLE actor_movie(
                actor_id INT REFERENCES actors(actorID),
                movie_id INT REFERENCES movies(movie_id),
                characterName text )
                """
            curObj.execute(actorMovieQuery)
            
            # Genres table created
            genresQuery = """
            CREATE TABLE genres(
                genres_id INT,
                genres_name text
            )
            """
            curObj.execute(genresQuery)

            # Genres movie relational table
            genresMovieQuery = """CREATE TABLE genres(
                genres_id INT REFERENCES genres(genres_name),
                movie_id INT REFERENCES movies(movie_id),
                )
                """
            curObj.execute(genresMovieQuery)

            # Crew table and  crew movie relational table
            crewQuery = """ CREATE TABLE crew(crewId INT PRIMARY KEY,
                    crewName text,
                    gender INT
            )
            """
            curObj.execute(crewQuery)

            crewMovieQuery = """CREATE TABLE crew_movie(
                crew_id INT REFERENCES crew(crewID),
                movie_id INT REFERENCES movies(movie_id),
                dept text )
                """
            curObj.execute(crewMovieQuery)

            # Production table and  Production movie relational table
            prodQuery = """ CREATE TABLE production(pId INT PRIMARY KEY,
                    pName text
            )
            """
            curObj.execute(prodQuery)

            prodMovieQuery = """CREATE TABLE prod_movie(
                pId INT REFERENCES production(pId),
                movie_id INT REFERENCES movies(movie_id),
                )
                """
            curObj.execute(prodMovieQuery)

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
    tbCreatorIns = tableCreator()
    tbCreatorIns.create_tables()
    