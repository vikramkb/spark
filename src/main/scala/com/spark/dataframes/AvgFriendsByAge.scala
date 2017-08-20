package com.spark.dataframes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object AvgFriendsByAge {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
        .master("local")
      .appName("AvgFriendsByAge - Spark SQL")
      .getOrCreate()

//    val spark = SparkSession
//      .builder()
//      .master("spark://vkurugun-mac.local:7077")
//      .appName("AvgFriendsByAge - Spark SQL")
//      .getOrCreate()

    spark.conf.set("spark.executor.memory", "2g")
    spark.conf.set("spark.cores.max", "1")

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val df = spark
      .read
      .option("header", value = true)
      .option("mode", "DROPMALFORMED")
      .option("inferSchema", "true")
      .csv("file:/Users/VikramBabu/open-source/practice/ml-20m/fakefriendswithheader.csv")

    df.groupBy("age")
      .agg(bround(avg("num_of_friends"), 2).alias("avg"))
      .orderBy("age")
      .show()
  }
}
