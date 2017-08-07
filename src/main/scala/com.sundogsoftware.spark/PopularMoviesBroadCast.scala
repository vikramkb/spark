package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark._

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object PopularMoviesBroadCast {

  def moviesDictionary(sc : SparkContext) = {
    val movies = sc.textFile("file:/Users/VikramBabu/open-source/practice/ml-20m/movies.csv")
    movies.mapPartitionsWithIndex((idx, iter) => if(idx == 0) iter.drop(1) else iter)
      .map(line => {
        val fields = line.split(",")
        (fields(0), fields(1))
      }).sortByKey().collectAsMap()
  }
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("PopularMovieBroadCast")


//    val sparkConf = new SparkConf()
//      .setMaster("spark://vkurugun-mac.local:7077")
//      .setAppName("Test")
//      .set("spark.executor.memory", "2g")
//      .set("spark.cores.max", "2")
    val sc = new SparkContext(sparkConf)
    val moviesDict = sc.broadcast(moviesDictionary(sc))

    val tags = sc.textFile("file:/Users/VikramBabu/open-source/practice/ml-20m/ratings.csv")
    val popularMovies = tags.mapPartitionsWithIndex((idx, iter) => if(idx == 0) iter.drop(1) else iter)
      .map(line => {
        val fields = line.split(",")
        val movieId = fields(1)
        val rating = fields(2).toFloat
        (movieId, rating)
      }).reduceByKey((r1, r2) => r1+r2)
      .map(p => (p._2, p._1))
      .sortByKey(ascending = false)
          .take(5)
    popularMovies.foreach(p => println(s"${moviesDict.value(p._2)} - ${p._1}"))

    Thread.sleep(10000000)

  }
}
