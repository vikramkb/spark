//sbt compile;sbt package;$SPARK_HOME/bin/spark-submit --class "com.spark.rdd.AvgFriendsByAge" --master spark://vkurugun-mac.local:7077 /Users/VikramBabu/open-source/practice/spark/target/scala-2.11/spark_2.11-1.0.jar
package com.spark.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object AvgFriendsByAge {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

//    val sparkConf = new SparkConf()
//      .setMaster("local[*]")
//      .setAppName("Avg Friends By Age : RDD")

    val sparkConf = new SparkConf()
      .setMaster("spark://vkurugun-mac.local:7077")
      .set("spark.executor.memory", "2g")
      .set("spark.cores.max", "1")
      .setAppName("Avg Friends By Age : RDD")

    val sc = new SparkContext(sparkConf)

    val tags = sc.textFile("file:/Users/VikramBabu/open-source/practice/ml-20m/fakefriends.csv")
    val avgByAge = tags.map(line => {
      val fields = line.split(",")
      (fields(2), fields(3).toInt)
    }).mapValues((x : Int) => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(x => x._1/x._2).collect()

    avgByAge.sorted.foreach(println)
    Thread.sleep(10000000)
  }
}
