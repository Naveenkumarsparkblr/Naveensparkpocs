package com.tcs.bu.bigdata.sparksql
import com.tcs.bu.bigdata.sparksql.allfunctions._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
// com.tcs.bu.bigdata.sparksql.jsonComplexData
object jsonComplexData {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("jsonComplexData").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
//val data = "E:\\WORK\\datasets\\world_bank.json"
    val data = args(0)
    val tab = args(1)
    val df = spark.read.format("json").load(data)
    df.createOrReplaceTempView("tab")
    // data cleaning process. means convert complex data type to simple datatype.
    // `_id`.`$oid` here ` its not single quote back quote (just under esc available)
    val query="""select  _id.`$oid` OID, approvalfy,board_approval_month,to_date(boardapprovaldate,'yyyy-MM-dd') boardapprovaldate, borrower,closingdate,country_namecode,countrycode, supplementprojectflg,countryname,countryshortname,envassesmentcategorycode,grantamt,ibrdcommamt,id,idacommamt,impagency,lendinginstr, lendinginstrtype,lendprojectcost, mjthemecode,sector.name[0] name1,sector.name[1] name2, sector.name[2] name3,sector1.name S1Name, sector1.percent S1Percent,sector2.name S2Name, sector2.percent S2Percent,sector3.name S3Name, sector3.percent S3Percent,sector4.name S4Name, sector4.percent S4Percent, prodline, prodlinetext, productlinetype,mjtheme[0] mjtheme1,mjtheme[1] mjtheme2,mjtheme[2] mjtheme3  , project_name ,projectfinancialtype, projectstatusdisplay,regionname,sectorcode, source,status,themecode,totalamt,totalcommamt,url,mp.Name mpname, mp.Percent mppercent, tn.code tncode, tn.name tnname,mn.code mncode, mn.name mnname, tc.code tccode, tc.name tcname, pd.DocDate pddocdate ,pd.DocType pddoctype, pd.DocTypeDesc pddoctypedesc, pd.DocURL  pddocurl, pd.EntityID pdid from tab lateral view explode(majorsector_percent) tmp as mp lateral view explode(theme_namecode) tmp as tn lateral view explode(mjsector_namecode) tmp as mn lateral view explode(mjtheme_namecode) tmp as tc lateral view explode(projectdocs) tmp as pd"""
    val pro  = spark.sql(query).cache()
    pro.printSchema()
    //pro.show()
    pro.createOrReplaceTempView("tab1")
    // process data
   // val res = spark.sql("select tcname, count(*) cnt from tab1 group by tcname order by cnt desc")
    //res.show()
pro.write.jdbc(ourl,tab,oprop)
    pro.write.mode(SaveMode.Overwrite).format("csv").option("header","true").save(s"s3://naveenbucket2020/output/$tab")
   //Writing in Hive database...
    //complete the arguments
   /* pro.write.format("hive").partitionBy("").bucketBy("")*/

    spark.stop()
  }
}