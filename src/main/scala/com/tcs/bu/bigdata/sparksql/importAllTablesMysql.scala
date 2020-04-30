package com.tcs.bu.bigdata.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object importAllTablesMysql {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("importAllTablesMysql").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    //Code shared by Venu 17-April-2020
    val mprop = new java.util.Properties()
    mprop.setProperty("user","musername")
    mprop.setProperty("password","mpassword")
    mprop.setProperty("driver","com.mysql.jdbc.Driver")
    val murl = "jdbc:mysql://tcsmysql.copceen3hfbj.ap-south-1.rds.amazonaws.com:3306/testdb"

    val qry = "(select table_name from information_schema.tables where TABLE_SCHEMA='dbmysql') t"
    val df = spark.read.jdbc(murl,s"$qry",mprop)
    df.show()
    // now convert dataframe to rdd using df.rdd ..
    val tabs = df.rdd.map(x=>x(0).toString).collect.toArray.filter(x=>(!x.endsWith("1")))
    tabs.foreach { x =>
      spark.sql("create database manasadb")
      val tab = x.toString()
      print(s"importing data from $tab table")
      val odf = spark.read.jdbc(murl, s"$tab", mprop)
      odf.show()
      odf.write.mode(SaveMode.Overwrite).jdbc(murl,s"$tab"+1,mprop)
    }

    spark.stop()
  }
}