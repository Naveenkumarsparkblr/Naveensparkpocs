package com.tcs.bu.bigdata.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.tcs.bu.bigdata.sparksql.allfunctions._
object getOracleMysql {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("getOracleMysql").enableHiveSupport().getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val tab = args(0)
    import spark.implicits._
    import spark.sql
    val df = spark.read.jdbc(ourl,"EMP",oprop)
    df.show()

    val mdf = spark.read.jdbc(murl,"DEPT",mprop)
    mdf.show()
    // processing
    df.createOrReplaceTempView("emp")
    mdf.createOrReplaceTempView("dept")
    val res = spark.sql("select e.empno,e.ename,e.job,e.sal, d.loc from emp e join dept d on e.deptno=d.deptno")
    res.show()
    res.write.mode(SaveMode.Overwrite).jdbc(msurl,tab,msprop)
    res.write.mode(SaveMode.Overwrite).format("hive").saveAsTable(tab)
  //  res.write.mode(SaveMode.Overwrite).format("csv").option("header","true").save(s"s3://harishankerbucket2020/output/$tab")
    // if u get Exception in thread "main" java.lang.ClassNotFoundException: oracle.jdbc.OracleDriver, its dependency problem

//spark-submit --class  com.tcs.bu.bigdata.sparksql.jsonComplexData --master local --jars $(echo s3://naveenbucket2020/input/drivers/*.jar | tr ' ' ',')  --deploy-mode client s3://naveenbucket2020/input/jars/sparkpocs_2.11-0.1.jar s3://naveenbucket2020/input/json/world_bank.json naveen_wb_tab

    spark.stop()
  }
}