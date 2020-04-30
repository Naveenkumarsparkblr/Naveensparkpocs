package com.tcs.bu.bigdata.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object test {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext// to create rdd

    import spark.implicits._
    import spark.sql
val data = "E:\\WORK\\datasets\\asl.txt"
    val aslrdd =sc.textFile(data)
    val head = aslrdd.first()
    val res = aslrdd.filter(x=>x!=head).map(x=>x.split(",")).map(x=>(x(0),x(1),x(2)))
    res.collect.foreach(println)
    spark.stop()
  }
}

//map: apply a logic on top of each and every element. in map input number of elements and output number of elements must be same.
// let eg: u have 4 elemetns, output also 4 elements.
//filter: apply a logic on top of each element based on bool values.