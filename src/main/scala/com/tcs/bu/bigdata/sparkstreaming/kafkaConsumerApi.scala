package com.tcs.bu.bigdata.sparkstreaming

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
//package com.tcs.bu.bigdata.sparkstreaming.kafkaConsumerApi
object kafkaConsumerApi {
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

    import spark.implicits._
    import spark.sql
    val topics = Array("indpak", "znaus")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    //topic,data
    //indpak,"venu,32,hyd"

    // create a dstream
    val lines = stream.map(record =>  record.value)
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

      blr.write.mode(SaveMode.Append).jdbc(ourl,"blrinfo123",oprop)
      del.write.mode(SaveMode.Append).jdbc(ourl,"delhiinfo123",oprop)
      //other.write.mode(SaveMode.Append).jdbc(ourl,"othercity",oprop)
    }

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

  }
}