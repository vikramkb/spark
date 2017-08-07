import org.apache.spark.{SparkConf, SparkContext}

object SimpleApp {
  def main(args: Array[String]): Unit = {
    val logFileTxt = "./build.sbt"
    val conf = new SparkConf().setAppName("Sample Application").setMaster("local[2]").set("spark.executor.memory","1g");
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFileTxt, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"lines with a: $numAs, lines with b: $numBs")
    sc.stop()
  }
}