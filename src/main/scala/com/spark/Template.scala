package com.spark

import org.apache.spark.SparkContext

object Template {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "RatingsCounter")

    //  val sparkConf = new SparkConf()
    //    .setMaster("spark://vkurugun-mac.local:7077")
    //    .setAppName("Test")
    //    .set("spark.executor.memory", "1g")
    //    .set("spark.cores.max", "4")
    //
    //  val sc = new SparkContext(sparkConf)

    val customerOrdersRDD = sc.textFile("file:/Users/VikramBabu/open-source/practice/ml-20m/customer-orders.csv")
    customerOrdersRDD.map(line => {
      val fields = line.split(",")
      val custId = fields(0)
      val spendAmt = fields(2).toFloat
      (custId, spendAmt)
    }).reduceByKey((sa1, sa2) => sa1 + sa2)
      .sortByKey()
      .collect()
      .foreach(kv => println(s"customer : ${kv._1}, spent : ${kv._2}"))
//    customerOrdersRDD.foreach(println(_))
  }
}
