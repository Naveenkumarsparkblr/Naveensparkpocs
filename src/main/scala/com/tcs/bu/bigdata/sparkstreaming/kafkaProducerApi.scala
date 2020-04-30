package com.tcs.bu.bigdata.sparkstreaming

import org.apache.kafka.clients.producer._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._

object kafkaProducerApi {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("kafkaProducerApi").getOrCreate()
     //   val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
  //  val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val topic: String = if (args.length > 0) args(0) else "indpak"

   // val spark = SparkSession.builder.master("local[2]").appName("kafka_wordcount").getOrCreate()
    import spark.implicits._
    import spark.sql
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    import scala.util.Try
    val path = "E:\\WORK\\datasets\\output\\weblogs"
    val data = spark.sparkContext.textFile(path)
    data.foreachPartition(rdd => {
      import java.util._

      val props = new java.util.Properties()
      //  props.put("metadata.broker.list", "localhost:9092")
      //      props.put("serializer.class", "kafka.serializer.StringEncoder")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      //props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
     // props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("bootstrap.servers", "localhost:9092")

      // import kafka.producer._
      // val config = new ProducerConfig(props)
      val producer = new KafkaProducer[String, String](props)

      rdd.foreach(x => {
        println(x)
        producer.send(new ProducerRecord[String, String](topic.toString(), x.toString)) //sending to kafka broker
        //(topic, "venu,32,hyd")
        //(indpak,"anu,56,mas")
        Thread.sleep(5000)
      })

    })
    ssc.start() // Start the computation
    ssc.awaitTermination()
  }
}