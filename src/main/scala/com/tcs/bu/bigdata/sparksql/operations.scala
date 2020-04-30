package com.tcs.bu.bigdata.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object operations {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("operations").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val data = "E:\\WORK\\datasets\\zips.json"

    val df = spark.read.format("json").option("path",data).option("inferSchema", "true").load()
    df.createOrReplaceTempView(viewName="tab")

   // val res = df.select( col = "*").where(condition = $"state"==="NY" && "pop">84143 && $"city"!="NEW YORK")
   val res = df.select( col = "*").where(condition = $"state"==="NY" && $"city"!="NEW YORK")
    res.show()
    spark.stop()
  }
}