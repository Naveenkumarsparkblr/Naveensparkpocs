package com.tcs.bu.bigdata.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object rddtoDF {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("rddtoDF").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
val data = "E:\\WORK\\datasets\\us-500.csv"
    val usrdd = sc.textFile(data)
    val skip = usrdd.first()
    val reg = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    val all = "[^a-zA-Z]"
// u r creating a structure to the data.
    import spark.implicits._
    import spark.sql
    val res = usrdd.filter(x=>x!=skip).map(x=>x.split(reg)).map(x=>(x(0).replaceAll("\"",""),x(1).replaceAll("\"",""),x(2).replaceAll("\"",""),x(3).replaceAll("\"",""),x(4).replaceAll("\"",""),x(5).replaceAll("\"",""),x(6).replaceAll(all,""),x(7).replaceAll("\"",""),x(8).replaceAll("\"",""),x(9).replaceAll("\"",""),x(10).replaceAll("\"",""),x(11).replaceAll("\"","")))
    val df = res.toDF("first_name","last_name","company_name","address","city","county","state","zip","phone1","phone2","email","web")
    // here .map(x=>(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11))) its array data convert to tuple. Tuple easily process data.
    df.createOrReplaceTempView("tab")
    //val res1 = spark.sql("select state,count(*) cnt from tab  group by state order by cnt desc")
    val res1 = spark.sql("select state, count(*)  from tab group by state having count(*) >1 ")
    res1.show()



    spark.stop()
  }
}