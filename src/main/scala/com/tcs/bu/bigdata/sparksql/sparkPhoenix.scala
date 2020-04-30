package com.tcs.bu.bigdata.sparksql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
//com.tcs.bu.bigdata.sparksql.sparkPhoenix
object sparkPhoenix {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkPhoenix").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val df = spark.read.format("org.apache.phoenix.spark").option("table","EMP").option("zkUrl","localhost:2181").load()
    val df1 =  spark.read.format("org.apache.phoenix.spark").option("table","DEPT").option("zkUrl","localhost:2181").load()

    val join = df.join(df1,$"fname"===$"name").drop($"fname")

    join.show()
    //before write create a tablein phoenix
    //create table jointab(id int, name varchar, lname, mobile int, email varchar)
    join.write.mode(SaveMode.Overwrite).format("org.apache.phoenix.spark").option("table","JOINTAB").option("zkUrl","localhost:2181").save()





    spark.stop()
  }
}