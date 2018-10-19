from pyspark import SparkContext
from pyspark.sql import SparkSession
from os import environ
from ConnectorHelper import ConnectorHelper
import DatabaseConnector
import datetime



class JsonParser:
    
    def __init__(self):
        self.foldername = os.environ["BUCKET_NAME"]
        # Spark Session creation
        self.spark = SparkSession \
            .builder \
            .master(environ["SPARK_HOST"]) \
            .appName("meta_info_processor") \
            .getOrCreate()
    
    def stop_spark(self):
        self.spark.stop()
    
    def genres_parsing(self,sqlContext):
        todayDate = datetime.datetime.today().strftime('%Y-%m-%d')
        year,month,day = todayDate.split('-')
        filename = 'genresMovie' + ''.join((year, month, day)) + '.json'
        path = 's3a://' + foldername + '/' + filename
        genresData = self.spark.read.json(path)
        genresData = genresData.rdd.map(lambda r: r.movie).collect()[0]
        genresData.registerTempTable("genreTb")

        genresDF = sqlContext.sql("SELECT genres.id as genres_id,genres.name as genres_name FROM genreTb")
        uniqueGenres= sqlContext.createDataFrame(genresDF, ['genres_id', 'genres_name']).collect()

        uniqueGenres = uniqueGenres.collect()
        DatabaseConnector.redshift_saver(spark, uniqueGenres, tbname="genres", \
                                                tmpdir='tmp', savemode='append')
        
        genresMovieDF = sqlContext.sql("SELECT genres.id as genres_id,movie.id as movie_id FROM genreTb")
        DatabaseConnector.redshift_saver(spark, genresMovieDF, tbname="genres_movie", \
                                                tmpdir='tmp', savemode='append')
    
    
    def actors_parsing(self,sqlContext):
        todayDate = datetime.datetime.today().strftime('%Y-%m-%d')
        year,month,day = todayDate.split('-')
        filename = 'castMovie' + ''.join((year, month, day)) + '.json'
        path = 's3a://' + foldername + '/' + filename
        castData = self.spark.read.json(path)
        castData = castData.rdd.map(lambda r: r.movie).collect()[0]
        castData.registerTempTable("castTb")

        

        actorsDF = sqlContext.sql("SELECT actor.id,actor.name,actor.gender FROM castTb")
        uniqueActorDet= sqlContext.createDataFrame(actorsDF, ['actorId', 'actorName','gender']).collect()

        uniqueActorDet = uniqueActorDet.collect()
        DatabaseConnector.redshift_saver(spark, uniqueActorDet, tbname="actors", \
                                                tmpdir='tmp', savemode='append')
        
        actorMovieDF = sqlContext.sql("SELECT actor.id as actor_id,movie.id as movie_id, actor.character as characterName FROM castTb")
        DatabaseConnector.redshift_saver(spark, actorMovieDF, tbname="actor_movie", \
                                                tmpdir='tmp', savemode='append')

    
    def crew_parsing(self,sqlContext):
        todayDate = datetime.datetime.today().strftime('%Y-%m-%d')
        year,month,day = todayDate.split('-')
        filename = 'crewMovie' + ''.join((year, month, day)) + '.json'
        path = 's3a://' + foldername + '/' + filename
        castData = self.spark.read.json(path)
        castData = castData.rdd.map(lambda r: r.movie).collect()[0]
        castData.registerTempTable("crewTb")

        crewDF = sqlContext.sql("SELECT crew.id,crew.name,crew.gender FROM crewTb")
        uniqueCrewDet= sqlContext.createDataFrame(crewDF, ['crewId', 'crewName','gender']).collect()

        uniqueCrewDet = uniqueCrewDet.collect()
        DatabaseConnector.redshift_saver(spark, uniqueCrewDet, tbname="crew", \
                                                tmpdir='tmp', savemode='append')
        
        crewMovieDF = sqlContext.sql("SELECT crew.id as crew_id,movie.id as movie_id, crew.job as dept FROM crewTb")
        DatabaseConnector.redshift_saver(spark, crewMovieDF, tbname="crew_movie", \
                                                tmpdir='tmp', savemode='append')
        



    def production_parsing(self,sqlContext):
        todayDate = datetime.datetime.today().strftime('%Y-%m-%d')
        year,month,day = todayDate.split('-')
        filename = 'prodMovie' + ''.join((year, month, day)) + '.json'
        path = 's3a://' + foldername + '/' + filename
        prodData = self.spark.read.json(path)
        prodData = castData.rdd.map(lambda r: r.movie).collect()[0]
        prodData.registerTempTable("prodTb")

        prodDF = sqlContext.sql("SELECT production.id as pId,production.name as pName FROM prodTb")
        uniqueProd= sqlContext.createDataFrame(prodDF, ['pId', 'pName']).collect()

        uniqueProd = uniqueProd.collect()
        DatabaseConnector.redshift_saver(spark, uniqueProd, tbname="production", \
                                            tmpdir='tmp', savemode='append')
    
        prodMovieDF = sqlContext.sql("SELECT production.id as pId,movie.id as movie_id FROM prodTb")
        DatabaseConnector.redshift_saver(spark, prodMovieDF, tbname="prod_movie", \
                                            tmpdir='tmp', savemode='append')


    def main(self):
        sqlContext = SQLContext(self.spark)
        genres_parsing(sqlContext)
        actors_parsing(sqlContext)
        crew_parsing(sqlContext)
        production_parsing(sqlContext)
        
if __name__ == "__main__":
    jsonInst = JsonParser()
    main()
    stop_spark()
