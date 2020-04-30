package com.tcs.bu.bigdata.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object usdata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("usdata").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
//val data = "E:\\WORK\\datasets\\us-500.csv"
  //  val op = "E:\\WORK\\datasets\\output\\us-500"
    val data = args(0)
    val op = args(1)
    val usrdd = sc.textFile(data)
    val skip = usrdd.first()
    val reg = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    val all = "[^a-zA-Z]"
   // val all = "\""
    //alapv4974c, 9847159150
   // val reg = ","
    // select state, count(*) cnt from tab group by state order by cnt desc
    val res = usrdd.filter(x=>x!=skip).map(x=>x.split(reg)).map(x=>(x(6).replaceAll(all,""),1)).reduceByKey((a,b)=>a+b).sortBy(x=>x._2,false)
    res.take(50).foreach(println)
    res.coalesce(1).saveAsTextFile(op)
    spark.stop()
  }
}
//Exception in thread "main" java.lang.ArrayIndexOutOfBoundsException: 0
// if u missed anything like args(0) or args(1) at that time  u ll get suh errors.
// how many input partitions available same number of output files u ll get to know how many peratitions use rdd.getNumPartitions
// to minimise that number of output files u ll get coalesce(1)