package batch

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import org.apache.spark.internal.Logging
import java.util.Base64

/**
 * Author: Daisy Chen
 * Perform statistical analysis operations on data in HBase using Spark
 *
 * 1） Calculate the visit count for each province in each country - Top 10
 * 2） Calculate the visit count for different browsers
 */
object AnalysisApp extends Logging {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("AnalysisApp").master("local[2]").getOrCreate()
    val day = "20190130"

    // Connect HBase
    val conf = new Configuration()
    conf.set("hbase.rootdir", "hdfs://localhost:9000/hbase")
    conf.set("hbase.zookeeper.quorum", "localhost:2181")

    val tableName = "access_" + day
    conf.set(TableInputFormat.INPUT_TABLE, tableName) // From which table to read the data
    val cf = "info"
    val scan = new Scan()
    // Set the column family to scan
    scan.addFamily(Bytes.toBytes(cf))
    // Set the columns to scan
    scan.addColumn(Bytes.toBytes(cf), Bytes.toBytes("country"))
    scan.addColumn(Bytes.toBytes(cf), Bytes.toBytes("province"))
    scan.addColumn(Bytes.toBytes(cf), Bytes.toBytes("browsername"))

    conf.set(TableInputFormat.SCAN, Base64.getEncoder.encodeToString(ProtobufUtil.toScan(scan).toByteArray))


    // Read data using Spark's newAPIHadoopRDD
    val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    hbaseRDD.cache()

    hbaseRDD.take(10).foreach(x => {
      val rowKey = Bytes.toString(x._1.get())

      for (cell <- x._2.rawCells()) {
        val cf = Bytes.toString(CellUtil.cloneFamily(cell))
        val qualifier = Bytes.toString(CellUtil.cloneQualifier(cell))
        val value = Bytes.toString(CellUtil.cloneValue(cell))

        println(s"$rowKey : $cf : $qualifier : $value")
      }
    })
    logError("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

    // 1. Calculate the visit count for each province in each country - Top 10
    // Function 1
    hbaseRDD.map(x => {
        val country = Bytes.toString(x._2.getValue(cf.getBytes, "country".getBytes))
        val province = Bytes.toString(x._2.getValue(cf.getBytes, "province".getBytes))

        ((country, province), 1)
      }).reduceByKey(_ + _)
      .map(x => (x._2, x._1)).sortByKey(false) // (hello,3)=>(3,hello)=>(hello,3)
      .map(x => (x._2, x._1)).take(10).foreach(println)

    // Function 2
    import spark.implicits._
    hbaseRDD.map(x => {
        val country = Bytes.toString(x._2.getValue(cf.getBytes, "country".getBytes))
        val province = Bytes.toString(x._2.getValue(cf.getBytes, "province".getBytes))
        CountryProvince(country, province)

      }).toDF.select("country", "province")
      .groupBy("country", "province").count().show(10, false)

    logError("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

    // 2. Calculate the visit count for different browsers
    // Function 1
    hbaseRDD.map(x => {
        val browsername = Bytes.toString(x._2.getValue(cf.getBytes, "browsername".getBytes))
        (browsername, 1)
      }).reduceByKey(_ + _)
      .map(x => (x._2, x._1)).sortByKey(false)
      .map(x => (x._2, x._1)).foreach(println)

    // Function 2
    hbaseRDD.map(x => {
      val browsername = Bytes.toString(x._2.getValue(cf.getBytes, "browsername".getBytes))
      Browser(browsername)
    }).toDF().select("browsername").groupBy("browsername").count().show(false)

    // Function 3
    hbaseRDD.map(x => {
      val browsername = Bytes.toString(x._2.getValue("o".getBytes, "browsername".getBytes))
      Browser(browsername)
    }).toDF().createOrReplaceTempView("tmp")

    spark.sql("select browsername,count(1) cnt from tmp group by browsername order by cnt desc").show(false)

    // Remove from cache
    hbaseRDD.unpersist(true)

    spark.stop()

  }


  case class CountryProvince(country: String, province: String)

  case class Browser(browsername: String)
}
