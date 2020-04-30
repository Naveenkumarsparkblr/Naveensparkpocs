package com.tcs.bu.bigdata.sparksql

import org.apache.spark.sql._

object jsondata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("jsondata").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    import spark.implicits._
    import spark.sql
    val data = "E:\\WORK\\datasets\\zips.json"
    val df = spark.read.format("json").load(data)
    df.createOrReplaceTempView("tab")
    val res = spark.sql("select * from tab where pop=(select max(pop) from tab)")
    res.show()

    spark.stop()
  }
}