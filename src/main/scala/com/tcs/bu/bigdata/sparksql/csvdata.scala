package com.tcs.bu.bigdata.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object csvdata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("csvdata").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "E:\\WORK\\datasets\\us-500.csv"
    val op  = "E:\\WORK\\datasets\\output\\usdata1"
//spark 1.6 version
val df = spark.read.format("csv").option("path",data).option("header", "true").option("inferSchema", "true").load()
    df.createOrReplaceTempView("test")
    //val query = "select * from test where email like '%gmail%' and state='NY'"
    val query = "select first_name, last_name, company_name, address, city, county, state, rpad(zip,5,0) zip, regexp_replace(phone1,'-','') phone, regexp_replace(phone2,'-','') mobile, email, web  from test"
    val res = spark.sql(query)
  //  res.show()
    val reg = ","
    //val res1 = df.withColumn("state", regexp_replace($"state",reg,""))
    //val res1 = df.withColumn("company_name", regexp_replace($"company_name",reg,""))
val res1 = df.withColumn("state", when($"state"==="OH","ohio").when($"state"==="CA","cali").otherwise($"state"))

    res1.show()
    res.write.mode(SaveMode.Overwrite).format("csv").option("header", "true").option("delimiter","\t").save(op)

    spark.stop()
  }
}