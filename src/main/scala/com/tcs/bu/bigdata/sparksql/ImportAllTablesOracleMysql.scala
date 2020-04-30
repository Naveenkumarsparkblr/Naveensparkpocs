package com.tcs.bu.bigdata.sparksql

import org.apache.hadoop.hdfs.web.resources.OverwriteParam
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ImportAllTablesOracleMysql {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("ImportAllTablesOracleMysql").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    //Below commented code is written during training
/*
    val ourl="jdbc:oracle:thin:@//mydb.c2dj1vzohp5n.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val oprop = new java.util.Properties()
    oprop.put("user","ousername")
    oprop.put("password","opassword")
    oprop.put("driver","oracle.jdbc.OracleDriver")
 val qry = "(select table_name from all_tables where TABLESPACE_NAME='USERS') T'
val df = spark.read.jdbc(ourl,table=s"$qry",oprop)
   val tabs = df.select($"TABLE_NAME").rdd.map(x=>x(0).toString.toUpperCase()).collect.toArray.filter(x=>x!="DATA_2008")

    // mysql
    val murl="jdbc:mysql://dbmysql.ckyod3xssluv.ap-south-1.rds.amazonaws.com:3306/dbmysql"
    val mprop = new java.util.Properties()
    mprop.put("user","musername")
    mprop.put("password","mpassword")
    mprop.put("driver","com.mysql.cj.jdbc.Driver")


   // val tabs = Array("EMP","DEPT","ASL")
    tabs.foreach { x=>
      val tab = x.toString()
      val odf = spark.read.jdbc(ourl, table = "$tab", oprop)
      odf.show()
      odf.write.mode(saveMode.overwri).jdbc(murl,table=s"$tab"+1,mprop)

    }*/

    //---17-April-2020---Code shared by Venu
    val oprop = new java.util.Properties()
    oprop.setProperty("user","ousername")
    oprop.setProperty("password","opassword")
    oprop.setProperty("driver","oracle.jdbc.driver.OracleDriver")
    val ourl = "jdbc:oracle:thin:@//mydb.c2dj1vzohp5n.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val mprop = new java.util.Properties()
    mprop.setProperty("user","musername")
    mprop.setProperty("password","mpassword")
    mprop.setProperty("driver","com.mysql.jdbc.Driver")
    val murl = "jdbc:mysql://dbmysql.ckyod3xssluv.ap-south-1.rds.amazonaws.com:3306/dbmysql"


    val qry = "(select table_name from all_tables WHERE TABLESPACE_NAME='USERS') t"
    val df = spark.read.jdbc(ourl,s"$qry",oprop)
    df.show()
    val tabs = df.rdd.map(x=>x(0).toString.toUpperCase()).collect.toArray.filter(x=>x!="DATA_2008")

    //val tabs = Array("EMP","DEPT","ASL")
    tabs.foreach{ x=>
      val tab = x.toString()
      print(s"importing data from $tab table")
      val odf = spark.read.jdbc(ourl,s"$tab",oprop)
      odf.show()
      odf.write.mode(SaveMode.Overwrite).jdbc(murl,s"$tab"+1,mprop)


    }


    spark.stop()
  }
}


