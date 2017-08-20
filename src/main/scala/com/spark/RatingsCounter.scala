package com.spark

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.SQLContext

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object RatingsCounter {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")

//    val sparkConf = new SparkConf()
//      .setMaster("spark://vkurugun-mac.local:7077")
//      .setAppName("Test")
//      .set("spark.executor.memory", "1g")
//      .set("spark.cores.max", "4")
//
//    val sc = new SparkContext(sparkConf)

    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("file:/Users/VikramBabu/open-source/practice/ml-large/movies.large.csv")
//    val lines = sc.parallelize(1 to 1000).collect()

    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val ratings = lines.map(_.toString().split(",")(2))
//
//    // Count up how many times each value (rating) occurs
    val results = ratings.countByValue()
//
//    // Sort the resulting map of (rating, count) tuples
    val sortedResults = results.toSeq.sortBy(_._1)
//
//    // Print each result on its own line.
    sortedResults.foreach(println)
    Thread.sleep(10000000)
  }
}
