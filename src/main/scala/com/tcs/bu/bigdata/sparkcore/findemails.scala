package com.tcs.bu.bigdata.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object findemails {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("findemails").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
//val data = "E:\\WORK\\datasets\\demochat.txt"
   val data = args(0)
    val rdd = sc.textFile(data)
    val res = rdd.flatMap(x=>x.split(" ")).filter(x=>x.contains("@"))
      res.take(10).foreach(println)

    spark.stop()
  }
}