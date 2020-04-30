package com.tcs.bu.bigdata.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object csvdata10k {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("csvdata10k").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "E:\\WORK\\datasets\\10000Records.csv"
    val df1 = spark.read.format("csv").option("path",data).option("dateFormat","MM/dd/yyyy").option("header", "true").option("inferSchema", "true").load()
    // data cleaning process
    //https://regexone.com/  --->regular expression
    //https://regexone.com/  --->regular expression
    val reg="[^a-zA-Z1-9]"
    val cols = df1.columns.map(x=>x.replaceAll(reg,"").toLowerCase)
    val ndf = df1.toDF(cols:_*) // data cleaning process
    //processing
ndf.createOrReplaceTempView("tab")
    val res = spark.sql("select * from tab")
    res.show()
    // //toDF() ...rdd converts to Dataframe use toDF() .... rename all columns at that time use toDF()

    ndf.printSchema()

    spark.stop()
  }
}