package com.tcs.bu.bigdata.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object dateFunctions {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("dateFunctions").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val df= spark.createDataFrame(Seq(
      (7369, "SMITH", "CLERK", 7902, "17-Dec-80", 800, 20, 10),
      (7499, "ALLEN", "SALESMAN", 7698, "20-Feb-81", 1600, 300, 30),
      (7521, "WARD", "SALESMAN", 7698, "22-Feb-81", 1250, 500, 30),
      (7566, "JONES", "MANAGER", 7839, "2-Apr-81", 2975, 0, 20),
      (7654, "MARTIN", "SALESMAN", 7698, "28-Sep-81", 1250, 1400, 30),
      (7698, "BLAKE", "MANAGER", 7839, "1-May-81", 2850, 0, 30),
      (7782, "CLARK", "MANAGER", 7839, "9-Jun-81", 2450, 0, 10),
      (7788, "SCOTT", "ANALYST", 7566, "19-Apr-87", 3000, 0, 20),
      (7839, "KING", "PRESIDENT", 0, "17-Nov-81", 5000, 0, 10),
      (7844, "TURNER", "SALESMAN", 7698, "8-Sep-81", 1500, 0, 30),
      (7876, "ADAMS", "CLERK", 7788, "23-May-87", 1100, 0, 20),
      (7877, "venu", "CLERK", 7788, "2-Apr-20", 1100, 0, 20),
        (7878, "naveen", "CLERK", 7788, "2-Apr-20", 1100, 0, 20)
    )).toDF("empno", "ename", "job", "mgr", "dob", "sal", "comm", "deptno").withColumn("dob",to_date($"dob","dd-MMM-yy"))
  //  df.show()
   // df.printSchema()
    df.createOrReplaceTempView("tab")
    //val res = spark.sql("select empno,ename,job,mgr,to_date(dob,'dd-MMM-yy') dob, sal,comm,deptno, current_date() todaydate, add_months(current_date(),100) after100months, date_add(current_date(),100) after100days,datediff(current_date(),to_date(dob,'dd-MMM-yy')) age_datediff, months_between( current_date(),to_date(dob,'dd-MMM-yy')) monthsbet, year(current_date())-year(to_date(dob,'dd-MMM-yy')) yrsbetw, next_day(current_date(),'Wed') nextwed, quarter(dob) quart,dayofweek(current_date()) dayofweek, last_day(dob) lasatday, datediff(current_date(),last_day(current_date())) togetsal from tab")
//Scala code for the above SQL query
    val res = df.withColumn("today",current_date()).withColumn("lastday",datediff(last_day(col("today")),col("today"))).withColumn("after100", date_add($"today",100))


res.show()
    res.printSchema()
    spark.stop()
  }
}