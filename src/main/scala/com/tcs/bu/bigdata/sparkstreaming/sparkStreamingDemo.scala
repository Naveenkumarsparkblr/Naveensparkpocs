package com.tcs.bu.bigdata.sparkstreaming

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._

//package com.tcs.bu.bigdata.sparkstreaming.sparkStreamingDemo

object sparkStreamingDemo {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[2]").appName("sparkStreamingDemo").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    //val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    // lines .. dstreams
    val lines = ssc.socketTextStream("ec2-13-127-107-63.ap-south-1.compute.amazonaws.com", 2222)
    lines.print()

    lines.foreachRDD { x =>

      // Get the singleton instance of SparkSession
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      // Convert RDD[String] to DataFrame
      val df = x.map(x=>x.split(",")).map(x=>(x(0),x(1),x(2))).toDF("name","age","city")
      df.show()
      // Create a temporary view
      df.createOrReplaceTempView("tab")

      val blr = spark.sql("select * from tab where city = 'blr'")
      val del = spark.sql("select * from tab where city = 'del'")
      //val other = spark.sql("select * from tab where city<> 'del' or city <> 'blr'")

      val ourl="jdbc:oracle:thin:@//mydb.c2dj1vzohp5n.ap-south-1.rds.amazonaws.com:1521/ORCL"
      val oprop = new java.util.Properties()
      oprop.put("user","ousername")
      oprop.put("password","opassword")
      oprop.put("driver","oracle.jdbc.OracleDriver")

      blr.write.mode(SaveMode.Append).jdbc(ourl,"blrinfo",oprop)
      del.write.mode(SaveMode.Append).jdbc(ourl,"delhiinfo",oprop)
      //other.write.mode(SaveMode.Append).jdbc(ourl,"othercity",oprop)
    }

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

  }
}
//  spark streaming ... sc ... to create rdd ...
// sqlContext .........sqlContext ... to create dataframe
//sparksession ........ spark ...... to create datasets
// spark Streaming Context ...ssc..... to create Dstreams