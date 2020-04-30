package com.tcs.bu.bigdata.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object sparkjson {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkjson").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val data = "E:\\WORK\\datasets\\zips.json"

    val df = spark.read.format("json").option("path",data).option("inferSchema", "true").load()
    df.createOrReplaceTempView(viewName="tab")
  // val res =spark.sql(sqlText = "select cast(_id as int)id,city,loc(0) lang ,loc(1) lat, pop,state from tab")
   val res =spark.sql(sqlText = "select city,pop,case when state='MA' then 'MAA' end state from tab")
    res.show()
    df.show()
    spark.stop()
  }
}