package com.tcs.bu.bigdata.sparkstreaming

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
object sparkTwitterDataAnalysis {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkTwitterDataAnalysis").getOrCreate()
        val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val APIkey = "bhEJMdprgSOEy4eZb04YI2eXT" //APIKey
    val APIsecretkey = "Fk1lsHNBuqIVgu1QSECswBIr4cuzOTDn0fvFXXrIWAxZcMO7Ws" // (API secret key)
    val Accesstoken = "181460431-SVbVapjTbAkRuIieFsqfgcnq9d3AbeiVD9xVxTHK" //Access token
    val Accesstokensecret = "wOehKqUUbUjzf8C6s5Ie2euytGOLg6xVBG2wc4Ax0NEbT" //Access token secret

    val searchFilter = "Corona, CoronaVirus, covid19"
    //  val pipelineFile = ""
    //val searchFilter = "spark, pyspark,scala,databricks"


    //  import spark.sqlContext.implicits._
    System.setProperty("twitter4j.oauth.consumerKey", APIkey)
    System.setProperty("twitter4j.oauth.consumerSecret", APIsecretkey)
    System.setProperty("twitter4j.oauth.accessToken", Accesstoken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", Accesstokensecret)
    //val lines = ssc.socketTextStream("localhost", 9999)

    // create dstreams

    val lines = TwitterUtils.createStream(ssc, None, Seq(searchFilter.toString)).filter(x=>x!=null)
    lines.foreachRDD { a =>


      // Get the singleton instance of SparkSession
      val spark = SparkSession.builder.config(a.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      // Convert RDD[String] to DataFrame
      val df = a.map(x=>(x.getUser().getScreenName(),x.getText(),x.getUser().getName())).toDF("screenname","msg","user")

val res = df.filter(!$"msg".like("RT %"))
//val res = df.filter("msg not like '%RT @%'")
      res.show(false)
      // Create a temporary view
      df.createOrReplaceTempView("tab")

      val who = spark.sql("select * from tab where msg like '%WHO%'")
     // val del = spark.sql("select * from tab where city = 'del'")
      //val other = spark.sql("select * from tab where city<> 'del' or city <> 'blr'")

      val ourl="jdbc:oracle:thin:@//mydb.c2dj1vzohp5n.ap-south-1.rds.amazonaws.com:1521/ORCL"
      val oprop = new java.util.Properties()
      oprop.put("user","ousername")
      oprop.put("password","opassword")
      oprop.put("driver","oracle.jdbc.OracleDriver")

      who.write.mode(SaveMode.Append).jdbc(ourl,"who_corona",oprop)
    //  del.write.mode(SaveMode.Append).jdbc(ourl,"delhiinfo",oprop)*/
      //other.write.mode(SaveMode.Append).jdbc(ourl,"othercity",oprop)
    }


    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

  }
}