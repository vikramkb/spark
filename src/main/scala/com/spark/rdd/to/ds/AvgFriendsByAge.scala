//sbt compile;sbt package;$SPARK_HOME/bin/spark-submit --class "com.spark.rdd.AvgFriendsByAge" --master spark://vkurugun-mac.local:7077 /Users/VikramBabu/open-source/practice/spark/target/scala-2.11/spark_2.11-1.0.jar
package com.spark.rdd.to.ds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, bround}
import org.apache.spark.{SparkConf, SparkContext}

object AvgFriendsByAge {

  case class FriendConnection(age: Int, numOfConnections: Int)

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

    import spark.implicits._

    val tags = spark.sparkContext.textFile("file:/Users/VikramBabu/open-source/practice/ml-20m/fakefriends.csv")
    val ds = tags.map(line => {
      val fields = line.split(",")
      FriendConnection(fields(2).toInt, fields(3).toInt)
    }).toDS

    ds.groupBy(ds("age"))
      .agg(bround(avg("numOfConnections"), 2).alias("avg"))
      .orderBy("age")
      .show()

//    Thread.sleep(10000000)
  }
}
