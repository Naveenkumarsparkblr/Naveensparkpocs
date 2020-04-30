package com.tcs.bu.bigdata.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object allfunctions {

    def oldageoffer(bal:Int,mar:String)  = (bal,mar) match {
      case (b,m) if(b>500 && m=="single") => "80% off"
      case (b,m) if(b>2000 && m=="divorced") => "70% offer"
      case (b,m) if(m=="married") => "10% off"
      case _ => "no offer"

  }
  val nudf = udf(oldageoffer _)

  val msurl="jdbc:sqlserver://venudb.cnwu9xc3aep7.ap-south-1.rds.amazonaws.com:1433;databaseName=venutasks;"
  val msprop = new java.util.Properties()
  msprop.put("user","msusername")
  msprop.put("password","mspassword")
  msprop.put("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")


  val murl="jdbc:mysql://dbmysql.ckyod3xssluv.ap-south-1.rds.amazonaws.com:3306/dbmysql"
  val mprop = new java.util.Properties()
  mprop.put("user","musername")
  mprop.put("password","mpassword")
  mprop.put("driver","com.mysql.cj.jdbc.Driver")

  val ourl="jdbc:oracle:thin:@//mydb.c2dj1vzohp5n.ap-south-1.rds.amazonaws.com:1521/ORCL"
  val oprop = new java.util.Properties()
  oprop.put("user","ousername")
  oprop.put("password","opassword")
  oprop.put("driver","oracle.jdbc.OracleDriver")
}