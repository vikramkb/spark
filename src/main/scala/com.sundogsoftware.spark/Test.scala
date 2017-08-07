package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark._

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Test {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "RatingsCounter")

//    val sparkConf = new SparkConf()
//      .setMaster("spark://vkurugun-mac.local:7077")
//        .setAppName("Test")
//        .set("spark.executor.memory", "2g")
//        .set("spark.cores.max", "2")
//      val sc = new SparkContext(sparkConf)


      val rdd1 = sc.parallelize(List(("a", 1), ("b",2))).map(x => (x._1, x._2))
      val rdd2 = sc.parallelize(List(("a", 3), ("b",4))).map(x => (x._1, x._2))
      rdd1.join(rdd2).collect().foreach(println)
//    sc.parallelize(List(("a",(1,2,3)), ("b",(1,2,3)), ("c",(1,2,3)))).flatMap(t => {
//      List((t._1, t._2._1),(t._1, t._2._2),(t._1, t._2._3))
//    }).collect().foreach(println)
    //    Thread.sleep(120000)
  }
}
