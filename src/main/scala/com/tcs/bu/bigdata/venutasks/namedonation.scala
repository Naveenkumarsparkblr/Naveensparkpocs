package com.tcs.bu.bigdata.venutasks

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object namedonation {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("namedonation").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val data = "E:\\WORK\\datasets\\namedonations.txt"
     val op = "E:\\WORK\\datasets\\output\\namedonations_op"
   // val data = args(0)
   // val op = args(1)
    val usrdd = sc.textFile(data)
    val skip = usrdd.first()


    val reg = ","
   // val res = usrdd.filter(x=>x!=skip).map(x=>x.split(reg)).map(x=>(x(1)).sortBy(x=>x._1,false)
    val res = usrdd.filter(x=>x!=skip).map(x=>x.split(reg)).map(x=>(x(1)))
     res.take(5).foreach(println)
   res.coalesce(1).saveAsTextFile(op)
      // res.saveAsTextFile(op)
    spark.stop()
  }
}