package com.sundogsoftware.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.Tuple2

object AvgFriendsByAge {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf = new SparkConf()
      .setMaster("spark://vkurugun-mac.local:7077")
      .setAppName("Test")
      .set("spark.executor.memory", "1g")
      .set("spark.cores.max", "4")

    val sc = new SparkContext(sparkConf)

    val tags = sc.textFile("file:/Users/VikramBabu/open-source/practice/ml-20m/fakefriends.csv")
    val avgByAge = tags.map(line => {
      val fields = line.split(",")
      (fields(2), fields(3).toInt)
    }).mapValues((x : Int) => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(x => x._1/x._2).collect()
    //    32, (100, 1)
    //    32, (110, 1)
    //    32, (120, 1)
    //    32, (90, 1)
    //    50, (10, 1)
    //    60, (10, 1)
//one to one mapping
// x to y conversion
//    32 => (100, 1), (110, 1) => (100+110, 1+1) => (210, 2)
//    32 => (120, 1), (90, 1) => (210, 2)
//    32 => (210, 2), (210, 2) => (420, 4)
//    RDD[(Int, Tuple2(Int, Int))]

    // 32 => 105

    // 10 -> 10
    // 100 -> 10



//    RDD[Int, Tuple2(Int, Int)]
//    32, (100, 1)
//    50, (10, 1)
//
//    Int 100 -> (100, 1) Tuple2(x: Int, x:Int)
//    10 -> (10, 1)
//    15 -> (15, 1)
//
//    32, 100
//    50, 10
//    34, 15
    avgByAge.sorted.foreach(println)
    Thread.sleep(10000000)
  }
}
