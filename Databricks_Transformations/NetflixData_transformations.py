# Databricks notebook source
# MAGIC
# MAGIC %md
# MAGIC <b>
# MAGIC
# MAGIC
# MAGIC 1. Best Movie by Year Netflix = this model contains the best movie by year. The criteria applied to consider this table is at least 25,000 votes ordered by imdb score ranking of 1.
# MAGIC
# MAGIC 2. Best Show by Year Netflix = this model contains the best tv show by year. The criteria applied to consider this table is at least 25,000 votes ordered by imdb score ranking of 1.
# MAGIC
# MAGIC 3. Best Movies Netflix = this model contains all the movies that pass the following criteria:
# MAGIC at least an imdb score of 6.9
# MAGIC at least 10,000 votes
# MAGIC
# MAGIC 4. Best Shows Netflix = this model contains all the tv shows that pass the following criteria:
# MAGIC at least an imdb score of 7.5
# MAGIC at least 10,000 votes 

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------


dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC MOUNTING ADLS LANDING CONTAINER IN DATABRICKS

# COMMAND ----------


import pyspark.sql.functions as F

#Just change all the values here based on the resource name you have created in your environemnt and workspace.

stgAccountSASTokenKey = 'sastokenforstgkey'
databricksScopeName ='project2'
storageContainer ='landing'
storageAccount='missionadestgacc468'
landingMountPoint ='/mnt/landing' ## You can provide your mnt point

# COMMAND ----------

if not any(mount.mountPoint == landingMountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount( source = 'wasbs://{}@{}.blob.core.windows.net'.format(storageContainer, storageAccount), mount_point= landingMountPoint, extra_configs ={'fs.azure.sas.{}.{}.blob.core.windows.net'.format(storageContainer,storageAccount):dbutils.secrets.get(scope = databricksScopeName, key= stgAccountSASTokenKey)})
    print('Mounted the storage account successfully')
else:
    print('Storage account already mounted')

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC READING CSV FILE FROM ADLS LANDING CONTAINER WITH SPECIFIC SCHEMA

# COMMAND ----------

credits_schema = "index int, person_id int, id string, name string, character string, role string "

# COMMAND ----------

df_credits = spark.read.csv('dbfs:/mnt/landing/input/raw_credits', schema=credits_schema, header= True, multiLine= True, escape="\'" )
display(df_credits) 

# COMMAND ----------

df_credits.count()

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC READING TITLES FILE USING CUSTOM SCHEMA

# COMMAND ----------

titles_schema = "index int,id string, title string, type string, release_year int, age_certification string, runtime int, genres string, production_countries string, seasons int, imdb_id string, imdb_score int,imdb_votes int "

# COMMAND ----------

df_titles = spark.read.csv('dbfs:/mnt/landing/input/raw_titles', schema=titles_schema, header= True, multiLine = True, escape = "\'")
display(df_titles)

# COMMAND ----------

df_titles.count()

# COMMAND ----------


df_credits.createOrReplaceTempView("credits")


# COMMAND ----------

df_titles.createOrReplaceTempView("titles")

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC JOINING CREDITS AND TITLES TABLES BASED ON COLUMN ID

# COMMAND ----------

df = spark.sql("select T.*, C.person_id, C.name, C.character, C.role from titles as T inner join credits as C on T.id = C.id")

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC CREATING TEMPARARY TABLE 

# COMMAND ----------

df.createOrReplaceTempView("netflix")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from netflix

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC Best Movie by Year Netflix : This model contains the best movie by year. The criteria applied to consider this table is at least 25,000 votes ordered by imdb score ranking of 1.

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (select title, release_year,imdb_votes, imdb_score, genres, production_countries,  row_number() over (partition by release_year order by imdb_score desc) as rank from netflix where type = 'MOVIE' and imdb_votes >25000 and imdb_score is not null )
# MAGIC
# MAGIC select * from cte where rank = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (
# MAGIC select title, release_year,imdb_votes, imdb_score, genres, production_countries,  row_number() over (partition by release_year order by imdb_score desc ) as rank from netflix where type = 'MOVIE' and imdb_votes >25000 and imdb_score is not null )
# MAGIC
# MAGIC select * from cte where rank = 1

# COMMAND ----------

df_bestMoviesByYear = spark.sql(" select * from ( select title, release_year,imdb_votes, imdb_score, genres, production_countries, \
                                 row_number() over (partition by release_year order by imdb_score desc ) as rank from netflix \
                                 where type = 'MOVIE' and imdb_votes >25000 and imdb_score is not null ) cte  \
                                where rank = 1 ")

df_bestMoviesByYear.count()

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC WRITING DATAFRAME TO ADLS AND ALSO SAVING AS DELTA TABLE

# COMMAND ----------

df_bestMoviesByYear.repartition(1).write.format('csv').option('header', True).mode('overwrite').option('path', '/mnt/landing/transformedData/bestMoviesByYear').saveAsTable('bestMoviesByYear')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bestmoviesbyyear

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC Best Show by Year Netflix : This model contains the best tv show by year. The criteria applied to consider this table is at least 25,000 votes ordered by imdb score ranking of 1.

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (select title, release_year, imdb_score, seasons as no_of_seasons, genres as main_genre, production_countries as main_production, row_number() over (partition by release_year order by imdb_score desc) as rank from netflix 
# MAGIC where type = 'SHOW' and imdb_votes >25000 and imdb_score is not null )
# MAGIC
# MAGIC select * from cte where rank =1

# COMMAND ----------


df_bestShowsByYear = spark.sql("select * from (select title, release_year, imdb_score, seasons as no_of_seasons, genres as main_genre, production_countries as main_production, row_number() over (partition by release_year order by imdb_score desc) as rank from netflix \
where type = 'SHOW' and imdb_votes >25000 and imdb_score is not null) cte where rank = 1 ")

df_bestShowsByYear.count()

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC WRITING DATAFRAME TO ADLS AND ALSO SAVING AS DELTA TABLE

# COMMAND ----------

df_bestShowsByYear.repartition(1).write.format('csv').option('header', True).mode('overwrite').option('path', '/mnt/landing/transformedData/bestShowsByYear').saveAsTable('bestShowsByYear')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bestShowsByYear

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC 3. Best Movies Netflix : this model contains all the movies that pass the following criteria:
# MAGIC at least an imdb score of 6.9 and at least 10,000 votes

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from titles

# COMMAND ----------

# MAGIC %sql
# MAGIC select title, release_year, imdb_score, runtime as Duration_in_mins, genres as main_genre, production_countries as main_production  from titles where type = 'MOVIE' and imdb_score >= 6.9 and imdb_votes >=10000

# COMMAND ----------

df_bestMovies = spark.sql("select title, release_year, imdb_score, runtime as Duration_in_mins, genres as main_genre, production_countries as main_production  from titles where type = 'MOVIE' and imdb_score >= 6.9 and imdb_votes >=10000 ")
df_bestMovies.count()

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC WRITING DATAFRAME TO ADLS AND ALSO SAVING AS DELTA TABLE

# COMMAND ----------

df_bestMovies.repartition(1).write.format('csv').option('header', True).mode('overwrite').option('path', '/mnt/landing/transformedData/bestMovies').saveAsTable('bestMovies')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bestMovies

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC 4. Best Shows Netflix = this model contains all the tv shows that pass the following criteria:
# MAGIC at least an imdb score of 7.5 and at least 10,000 votes 

# COMMAND ----------

# MAGIC %sql
# MAGIC select title, release_year, imdb_score, imdb_votes as no_of_votes,  runtime as Duration_in_mins, seasons as no_of_seasons, genres as main_genre, production_countries as main_production  from titles where type = 'SHOW' and imdb_score >= 7.5 and imdb_votes >=10000

# COMMAND ----------

df_bestShows = spark.sql(" select title, release_year, imdb_score, imdb_votes as no_of_votes,  runtime as Duration_in_mins, seasons as no_of_seasons, genres as main_genre, production_countries as main_production  from titles where type = 'SHOW' and imdb_score >= 7.5 and imdb_votes >=10000 ")
df_bestShows.count()

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC WRITING DATAFRAME TO ADLS AND ALSO SAVING AS DELTA TABLE

# COMMAND ----------

df_bestShows.repartition(1).write.format('csv').option('header', True).mode('overwrite').option('path', '/mnt/landing/transformedData/bestShows').saveAsTable('bestShows')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bestshows

# COMMAND ----------

