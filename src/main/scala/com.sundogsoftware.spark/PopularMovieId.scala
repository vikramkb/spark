package com.sundogsoftware.spark

package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.SparkSession

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object PopularMovieId {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConfig = new SparkConf()
      .setMaster("local[*]")
      .setAppName("PopularMovie")


//    val sparkConf = new SparkConf()
//      .setMaster("spark://vkurugun-mac.local:7077")
//      .setAppName("Test")
//      .set("spark.executor.memory", "2g")
//      .set("spark.cores.max", "2")
    val sc = new SparkContext(sparkConfig)

    val tags = sc.textFile("file:/Users/VikramBabu/open-source/practice/ml-20m/ratings.csv")
    val popularMovie = tags.mapPartitionsWithIndex((idx, iter) => if(idx == 0) iter.drop(1) else iter)
      .map(line => {
        val fields = line.split(",")
        val movieId = fields(1)
        val rating = fields(2).toFloat
        (movieId, rating)
      }).reduceByKey((r1, r2) => r1+r2)
        .reduce((kv1, kv2) => if (kv1._2 > kv2._2) kv1 else kv2)
    println(s"Popular Movie is ${popularMovie._1}")
  }
}
