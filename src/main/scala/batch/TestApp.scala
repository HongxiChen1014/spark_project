package batch

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.SparkSession

import java.util.{Date, Locale}

object TestApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("TestApp").master("local[2]").getOrCreate()
    System.setProperty("icode", "7323F3C73577877C")
    var logDF = spark.read.format("com.imooc.bigdata.spark.pk").option("path", "/Users/daisychen/Desktop/github/spark_project/src/data/test-access.log")
      .load()

    //    logDF.show(false)

    import org.apache.spark.sql.functions._
    def formatTime() = udf((time: String) => {
      FastDateFormat.getInstance("yyyyMMddHHmm").format(
        new Date(FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
          .parse(time.substring(time.indexOf("[") + 1, time.lastIndexOf("]"))).getTime
        ))
    })

    // update and adjust format
    logDF = logDF.withColumn("formattime", formatTime()(logDF("time")))

    logDF.show(false)

    spark.stop()
  }

}
