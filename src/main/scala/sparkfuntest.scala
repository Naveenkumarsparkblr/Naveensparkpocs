import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object sparkfuntest {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkfuntest").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val data = "E:\\WORK\\datasets\\asl.txt"
    val df = spark.read.format("csv").option("header","true").load(data)
    df.createOrReplaceTempView("tab")

    def agefunc(age:Int) = {
      if(age>0 && age<10) "10% off"
      else if (age>=10 && age<=18) "20% off"
      else if (age>18 && age<60) "30% off"
      else if (age>=60 && age<100) "50% off"
      else "no offer"
    }
    spark.udf.register("agetest", agefunc _)
    val res = spark.sql("select *, agetest(age) offer from tab")
    res.show()

    spark.stop()
  }
}

//----PRODUCT Disount
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
/*val data = "E:\\WORK\\datasets\\product.txt"
val df = spark.read.format("csv").option("header","true").load(data)
df.createOrReplaceTempView("tab")*/
