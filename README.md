# Data Enigneering Project : Movie Insights
**Analytics platform for Movie Investors and Movie Lovers**

## Motivation :
* The motion picture industry is growing at a rapid growth rate, likely due to the acceleration of online and mobile distribution, lower admission prices, and government policy initiatives.
* This industry is also rich in data, thus making it extremely exciting for statisticians and Analysts.
* Stakeholders are looking for a ‘magic formula’ to better understand and predict box office success are turning to statisticians and data scientists to help with this challenge.
* To increase their profits, producers and directors need to understand what raises the curiosity of their target audience. This is where analytics can play an effective role.
* Analyzing IMDB/Rotten Tomatoes, monthly collection reports of similar genre, and star cast help to take better decision where profit can be maximized.

## Tools and technologies used:
1. S3
2. Apache Spark
3. AWS Redshift
4. Node JS
5. Canvas JS
6. Airflow

## Data Extraction

* Movies basic data like release date, Movie Name, Genre, Production Company etc. are scraped from IMDB/Rotten tomatoes.
* I used scrapy from scraping the data and stored the data in Amazon s3.



**Data Pipeline**
![alt text](https://github.com/rohith28/MovieInsights/blob/master/img/DataPipeline.png)



**Database Schema**
![alt text](https://github.com/rohith28/MovieInsights/blob/master/img/schema.png)
