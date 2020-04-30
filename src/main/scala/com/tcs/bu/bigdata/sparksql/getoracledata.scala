package com.tcs.bu.bigdata.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object getoracledata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("getoracledata").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val today = date_sub(current_date(),1)
    val url="jdbc:oracle:thin:@//paritoshoracle.czhyzqz8vleo.ap-south-1.rds.amazonaws.com:1521/orcl"
    val prop =new java.util.Properties()
    prop.setProperty("user","ousername")
    prop.setProperty("password","opassword")
    prop.setProperty("driver","oracle.jdbc.OracleDriver")
    val pastdf=spark.read.jdbc(url,"EMP1",prop)
    val newdf =pastdf.withColumn("datecol",current_date())
    newdf.show()
    newdf.createOrReplaceTempView("tab")
    val res = spark.sql("select * from tab where HIREDATE>=date_sub(datecol,1)").drop("datecol")
    res.show()
    spark.stop()
  }
}