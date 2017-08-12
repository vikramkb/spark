package com.spark

import org.apache.spark.SparkContext

object WordCountInBook {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "RatingsCounter")

    //  val sparkConf = new SparkConf()
    //    .setMaster("spark://vkurugun-mac.local:7077")
    //    .setAppName("Test")
    //    .set("spark.executor.memory", "1g")
    //    .set("spark.cores.max", "4")
    //
    //  val sc = new SparkContext(sparkConf)

    val bookRDD = sc.textFile("file:/Users/VikramBabu/open-source/practice/ml-20m/book.txt")

    //    This method should only be used if the resulting map is expected to be small
    val wordCount = bookRDD.flatMap(line => line.split("\\W+"))
        .map(w => w.toLowerCase)
      .countByValue()
    wordCount.toSeq.sortBy(_._2).foreach(println)

    //To handle very large results, consider using reduceByKey
    bookRDD.flatMap(line => line.split("\\W+"))
      .map(w => (w.toLowerCase, 1))
        .reduceByKey((c1, c2) => c1+c2)
            .map(kv => (kv._2, kv._1))
              .sortByKey() //Sort the RDD by key, so that each partition contains a sorted range of the elements.
                  .collect() //collect will return or output an ordered list of records
                    .foreach(kv => println(s"${kv._1} - ${kv._2}"))
  }
}
