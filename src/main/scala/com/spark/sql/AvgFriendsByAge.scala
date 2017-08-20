package com.spark.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object AvgFriendsByAge {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .master("spark://vkurugun-mac.local:7077")
      .appName("AvgFriendsByAge - Spark SQL")
      .getOrCreate()

    spark.conf.set("spark.executor.memory", "2g")
    spark.conf.set("spark.cores.max", "1")

    import spark.implicits._

    val df = spark
              .read
              .option("header", value = true)
              .option("mode", "DROPMALFORMED")
              .option("inferSchema", "true")
              .csv("file:/Users/VikramBabu/open-source/practice/ml-20m/fakefriendswithheader.csv")
    df.printSchema()

    df.createTempView("friends")
    spark.sql("select age, cast(avg(num_of_friends) as decimal(36,2)) as avg from friends group by age order by age").show(1000)

    Thread.sleep(10000000)
  }
}
