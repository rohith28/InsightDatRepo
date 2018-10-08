y  # !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 25 16:50:19 2018

@author: rohith
"""

import time
import sys
from connectorHelper import connectorHelper


class tableCreator():

    def create_cursor(conn):
        cur = conn.cursor()
        print("connection esatblished")
        return cur
    
    def colse_cursor(curObj):
        curObj.close()
        print("Cursor closed")
        

    def create_tables():
        cnt =0
        print("Creating Tables ")
        start_time = time.time()
        curObj = create_cursor(conn)
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
           

            # Actor table actor movie relational table
            actorQuery = """CREATE TABLE actors(
                    actorId INT PRIMARY KEY,
                    actorName text,
                    gender INT)
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
                genres_id INT PRIMARY KEY,
                genres_name text
            )
            """
            curObj.execute(genresQuery)
           

            # Genres movie relational table
            genresMovieQuery = """CREATE TABLE genres_movie(
                genres_id INT REFERENCES genres(genres_id),
                movie_id INT REFERENCES movies(movie_id)
                )
            """
            curObj.execute(genresMovieQuery)
            

            # Crew table and  crew movie relational table
            crewQuery = """ CREATE TABLE crew(
                    crewId INT PRIMARY KEY,
                    crewName text,
                    gender INT)
            """
            curObj.execute(crewQuery)
           
            crewMovieQuery = """CREATE TABLE crew_movie(
                crew_id INT REFERENCES crew(crewID),
                movie_id INT REFERENCES movies(movie_id),
                dept text )
                """
            curObj.execute(crewMovieQuery)
            

            # Crew table and  crew movie relational table
            prodQuery = """ CREATE TABLE production(pId INT PRIMARY KEY,
                    pName text
            )
            """
            curObj.execute(prodQuery)
            
            prodMovieQuery = """CREATE TABLE prod_movie(
                pId INT REFERENCES production(pId),
                movie_id INT REFERENCES movies(movie_id)
                )
                """
            curObj.execute(prodMovieQuery)
            

        except psycopg2.ProgrammingError as err:
            print(err)
        except:
            print("Error in the creating tables")
            print("Unexpected error:", sys.exc_info()[0])
        curObj.close()
        conn.commit()
        print("Time taken to execute %s" % (time.time() - start_time))
        print("problem with %s", cnt)


if __name__ == "__main__":
    tbCreatorIns = tableCreator()
    tbCreatorIns.create_tables()
    