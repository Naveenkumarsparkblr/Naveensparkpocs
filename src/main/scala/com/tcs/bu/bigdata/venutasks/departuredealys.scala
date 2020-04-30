package com.tcs.bu.bigdata.venutasks

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object departuredealys {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("departuredealys").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val data = "E:\\WORK\\datasets\\departuredelays.csv"
    val op  = "E:\\WORK\\datasets\\output\\departuredelays"

    val df = spark.read.format("csv").option("path",data).option("dateformat","MM/dd/yyyy").option("header", "true").option("inferSchema", "true").load()
    df.createOrReplaceTempView("departdelay")
    val query = "select * from departdelay where destination='ATL' "
    val res = spark.sql(query)
    df.printSchema()
    res.write.mode(SaveMode.Overwrite).format("csv").option("header", "true").option("delimiter","\t").save(op)
    spark.stop()
  }
}