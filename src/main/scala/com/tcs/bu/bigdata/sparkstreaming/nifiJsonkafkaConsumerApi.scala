package com.tcs.bu.bigdata.sparkstreaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

//package com.tcs.bu.bigdata.sparkstreaming.kafkaConsumerApi
object nifiJsonkafkaConsumerApi {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("kafkaConsumerApi").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("indiapak")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    //topic,data
    //indpak,"venu,32,hyd"

    // create a dstream
    val lines = stream.map(record =>  record.value)
    lines.print()
    lines.foreachRDD { x =>


      // Get the singleton instance of SparkSession
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      // Convert RDD[String] to DataFrame
      val fst = x.first()
      val df = spark.read.json(x)
      df.createOrReplaceTempView("tab")
      val res = spark.sql("select nationality, r.user.name.first name, r.user.email, r.user.cell, r.user.location.city from tab lateral view explode(results) t as r ")

      res.show()
      res.printSchema()
      // Create a temporary view
      df.createOrReplaceTempView("tab")


      //val other = spark.sql("select * from tab where city<> 'del' or city <> 'blr'")

      val ourl="jdbc:oracle:thin:@//mydb.c2dj1vzohp5n.ap-south-1.rds.amazonaws.com:1521/ORCL"
      val oprop = new java.util.Properties()
      oprop.put("user","ousername")
      oprop.put("password","opassword")
      oprop.put("driver","oracle.jdbc.OracleDriver")

      res.write.mode(SaveMode.Append).jdbc(ourl,"nifiJsonData",oprop)

      //other.write.mode(SaveMode.Append).jdbc(ourl,"othercity",oprop)
    }

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

  }
}