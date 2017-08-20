package com.spark.dataset

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import sun.security.krb5.internal.SeqNumber

object AvgFriendsByAge {

  case class FriendConnection(age: Int, num_of_friends: Int)

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
    val ds = spark
      .read
      .option("header", value = true)
      .option("mode", "DROPMALFORMED")
      .option("inferSchema", "true")
      .csv("file:/Users/VikramBabu/open-source/practice/ml-20m/fakefriendswithheader.csv")
        .select("age", "num_of_friends")
        .as[FriendConnection]

    ds.printSchema()
    ds.groupBy("age")
      .agg(bround(avg("num_of_friends"), 2).alias("avg"))
      .orderBy("age")
      .show()

    ds.map(conn => (conn.age, (conn.num_of_friends, 1)))
      .groupByKey(_._1)
        .reduceGroups((x, y) => (x._1, (x._2._1+y._2._1, x._2._2 + y._2._2)))
          .map(_._2)
            .map(x => (x._1, Math.round(x._2._1.toDouble / x._2._2 * 100)/100.0))
              .rdd
                .sortByKey()
                  .collect()
                    .foreach(println)
  }
}
