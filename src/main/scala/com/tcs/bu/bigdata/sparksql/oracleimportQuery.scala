package com.tcs.bu.bigdata.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object oracleimportQuery {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("oracleomportquery").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
//Code written during training
/*    val oprop = new java.util.Properties()
    oprop.put("user","ousername")
    oprop.put("password","opassword")
    oprop.put("driver","oracle.jdbc.OracleDriver")
    val ourl="jdbc:oracle:thin:@//mydb.c2dj1vzohp5n.ap-south-1.rds.amazonaws.com:1521/ORCL"

    val query = "(select * from emp where sal>2800 and job<> 'MANAGER') t"

    val qdf = spark.read.jdbc(ourl,query,oprop)
    qdf.show()*/

//code shared by venu 17-April-2020
val oprop = new java.util.Properties()
    oprop.setProperty("user","ousername")
    oprop.setProperty("password","opassword")
    oprop.setProperty("driver","oracle.jdbc.driver.OracleDriver")
    val ourl = "jdbc:oracle:thin:@//mydb.c2dj1vzohp5n.ap-south-1.rds.amazonaws.com:1521/ORCL"

    val query = "(select * from emp where sal>2800 and job <> 'MANAGER') t"
    val qdf = spark.read.jdbc(ourl,query,oprop)

    qdf.show()
    spark.stop()
  }
}