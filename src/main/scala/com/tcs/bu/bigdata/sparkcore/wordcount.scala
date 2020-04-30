package com.tcs.bu.bigdata.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object wordcount {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("rddtoDF").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val data = "E:\\WORK\\datasets\\sample.txt"
    val rdd = sc.textFile(data)
    val res = rdd.flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey((a,b)=>a+b).sortBy(x=>x._2,false)
res.take(20).foreach(println)
    spark.stop()
  }
}
// map apply a logic on top of each and every element. number of input elements length and output element length must be same
//means if u have 10 elements output also 10 elements ull get.
