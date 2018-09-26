// Databricks notebook source
// MAGIC %md 
// MAGIC #Learning Databricks Professionally

// COMMAND ----------

val defaultMoviesUrl = "https://advtrainsb.blob.core.windows.net/data/movies.csv"
val defaultRatingsUrl = "adl://advtraindatalakestorage.azuredatalakestore.net/ratings.csv"

val moviesUrl = dbutils.widgets.text("moviesUrl","")
val ratingsUrl = dbutils.widgets.text("ratingsUrl", "")

var inputMoviesUrl = dbutils.widgets.get("moviesUrl")

if(inputMoviesUrl == null || inputMoviesUrl == "") {
  inputMoviesUrl = defaultMoviesUrl
}

var inputRatingsUrl = dbutils.widgets.get("ratingsUrl")

if(inputRatingsUrl == null || inputRatingsUrl == "") {
  inputRatingsUrl = defaultRatingsUrl
}


// COMMAND ----------

package com.microsoft.analytics.utils

import scala.io.Source
import scala.io.Codec
import java.nio.charset.CodingErrorAction

object MovieUtils {
  def loadMovieNames(fileName: String): Map[Int, String] = {
  if(fileName == null || fileName == "") {
    throw new Exception("Invalid File / Reference URL Specified!");
  }

  implicit val codec = Codec("UTF-8")

  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

  val lines = Source.fromURL(fileName).getLines

  lines.drop(1)

  var movieNames: Map[Int, String] = Map()

  for(line <- lines) {
    val records = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")
    val movieId = records(0).toInt
    val movieName = records(1)

    movieNames += (movieId -> movieName)
  }

  movieNames
}  
}

// COMMAND ----------

import com.microsoft.analytics.utils._

val broadcastedMovies = sc.broadcast( () => {MovieUtils.loadMovieNames(inputMoviesUrl)})

// COMMAND ----------

println(inputRatingsUrl)

// COMMAND ----------

spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id", "f84edbce-5851-4da4-892c-22f113aa54a5")
spark.conf.set("dfs.adls.oauth2.credential", "AqoKGIXmAj3tMu1ZVbLVqj1MxAssJ1yJKDtp9JSmHVc=")
spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")



spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.access.token.provider.type", spark.conf.get("dfs.adls.oauth2.access.token.provider.type"))
spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.client.id", spark.conf.get("dfs.adls.oauth2.client.id"))

spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.credential", spark.conf.get("dfs.adls.oauth2.credential"))

spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.refresh.url", spark.conf.get("dfs.adls.oauth2.refresh.url"))

val ratingsData = sc.textFile(inputRatingsUrl)
val originalData = ratingsData.mapPartitionsWithIndex((index, iterator) => {
if(index == 0) iterator.drop(1)

 else iterator
})
val mappedData = originalData.map(line => { val splitted = line.split(",")

(splitted(1).toInt, 1)
})
val reducedData = mappedData.reduceByKey((x, y) => (x + y))
val result = reducedData.sortBy(_._2).collect
val finalOutput = result.reverse.take(10)
val mappedFinalOuptut = finalOutput.map(record => (broadcastedMovies.value()(record._1), record._2))

// COMMAND ----------

mappedFinalOuptut.foreach(println)

// COMMAND ----------

case class MovieResult (movieName: String, noOfHits: Int)

val movieResults = mappedFinalOuptut.map(result => MovieResult(result._1, result._2))
val moviesRDD = sc.parallelize(movieResults)
val dataFrame = spark.sqlContext.createDataFrame(moviesRDD)

display(dataFrame)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ![Azure Data Bricks](https://azure.microsoft.com/svghandler/databricks?width=600&height=315)

// COMMAND ----------

// MAGIC 
// MAGIC %md
// MAGIC 
// MAGIC \\(c = \\pm\\sqrt{a^2 + b^2} \\)
// MAGIC 
// MAGIC \\( f(\beta)= -Y_t^T X_t \beta + \sum log( 1+{e}^{X_t\bullet\beta}) + \frac{1}{2}\delta^t S_t^{-1}\delta\\)
// MAGIC 
// MAGIC where \\(\delta=(\beta - \mu_{t-1})\\)
// MAGIC 
// MAGIC $$\sum_{i=0}^n i^2 = \frac{(n^2+n)(2n+1)}{6}$$