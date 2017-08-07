package com.sundogsoftware.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object MinTemparature {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "RatingsCounter")

  //  val sparkConf = new SparkConf()
  //    .setMaster("spark://vkurugun-mac.local:7077")
  //    .setAppName("Test")
  //    .set("spark.executor.memory", "1g")
  //    .set("spark.cores.max", "4")
  //
  //  val sc = new SparkContext(sparkConf)

    val temparatureFileRDD = sc.textFile("file:/Users/VikramBabu/open-source/practice/ml-20m/1800.csv")

    val minTempByStation = temparatureFileRDD.map(line => {
      val fields = line.split(",")
      val stationId = fields(0)
      val entryType = fields(2)
      val temp = fields(3).toInt / 10.0
      (stationId, entryType, temp)
    }).filter(tuple => tuple._2 == "TMIN")
      .map(tup3 => (tup3._1, tup3._3))
        .reduceByKey((x, y) => Math.min(x, y))
          .collect()


    minTempByStation.toSeq.sorted.foreach(println)
  }
}