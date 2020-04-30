package com.tcs.bu.bigdata.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.tcs.bu.bigdata.sparksql.allfunctions._
object Sparkfunctions {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("Sparkfunctions").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val data = "E:\\WORK\\datasets\\bank-full.csv"
    val df = spark.read.format(source ="csv").option("header","true").option("delimiter",";").option("inferschema","true").load(data)
    df.createOrReplaceTempView(viewName = "tab")
    //val res = spark.sql(sqlText = "select * from tab where age > 80 and age <99 and housing ='no' ")
    //val res = spark.sql(sqlText = "select *,concat_ws( '_',job,marital)fullname from tab")
    val res = spark.sql(sqlText = "select job,age,concat_ws( '_',job,marital)fullname from tab")
    df.show()
    res.show()
    df.printSchema()
    spark.udf.register("test",nudf)

//val res1 = df.withColumn("newoffer",nudf($"balance",$"marital"))
    val res1 = spark.sql("select *, test(balance,marital)  todayoffer from tab")

    res1.show()

    spark.stop()
  }
}