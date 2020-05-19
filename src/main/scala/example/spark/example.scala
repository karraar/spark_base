package example.spark

import example.spark.dataframe.Metrics
import org.apache.spark.sql.SparkSession


object SparkBase extends App {
    val spark = SparkSession
      .builder
        .master("local[*]")
      .appName("GroupBy Test")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val d = spark.read.json("src/main/resources/iss.json")
    val dfMetrics = new Metrics(d)
    println(dfMetrics.toString)
    spark.close()
}