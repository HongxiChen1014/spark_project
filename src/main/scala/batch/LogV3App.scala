package batch

import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, KeyValue, TableName}
import org.apache.hadoop.mapreduce.{Job => NewAPIHadoopJob}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import java.util.zip.CRC32
import java.util.{Date, Locale}
import scala.collection.mutable.ListBuffer

object LogV3App extends Logging {

  def main(args: Array[String]): Unit = {


    //    if (args.length != 1) {
    //      println("Usage: LogApp <time>")
    //      System.exit(1)
    //    }

    val day = "20190130"
    //    val day = args(0)

    val input = s"hdfs://localhost:9000/access/$day/*"
    val spark = SparkSession.builder().config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").appName("LogApp").master("local[2]").getOrCreate()
    System.setProperty("icode", "7323F3C73577877C")
    var logDF = spark.read.format("com.imooc.bigdata.spark.pk").option("path", "/Users/daisychen/Desktop/github/spark_project/src/data/test-access.log")
      .load()


    // UDF function
    import org.apache.spark.sql.functions._
    def formatTime() = udf((time: String) => {
      FastDateFormat.getInstance("yyyyMMddHHmm").format(
        new Date(FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
          .parse(time.substring(time.indexOf("[") + 1, time.lastIndexOf("]"))).getTime
        ))
    })

    logDF = logDF.withColumn("formattime", formatTime()(logDF("time")))


    val hbaseInfoRDD = logDF.rdd.mapPartitions(partition => {
      partition.flatMap(x => {
        val ip = x.getAs[String]("ip")
        val country = x.getAs[String]("country")
        val province = x.getAs[String]("province")
        val city = x.getAs[String]("city")
        val formattime = x.getAs[String]("formattime")
        val method = x.getAs[String]("method")
        val url = x.getAs[String]("url")
        val protocal = x.getAs[String]("protocal")
        val status = x.getAs[String]("status")
        val bytessent = x.getAs[String]("bytessent")
        val referer = x.getAs[String]("referer")
        val browsername = x.getAs[String]("browsername")
        val browserversion = x.getAs[String]("browserversion")
        val osname = x.getAs[String]("osname")
        val osversion = x.getAs[String]("osversion")
        val ua = x.getAs[String]("ua")

        val columns = scala.collection.mutable.HashMap[String, String]()
        columns.put("ip", ip)
        columns.put("country", country)
        columns.put("province", province)
        columns.put("city", city)
        columns.put("formattime", formattime)
        columns.put("method", method)
        columns.put("url", url)
        columns.put("protocal", protocal)
        columns.put("status", status)
        columns.put("bytessent", bytessent)
        columns.put("referer", referer)
        columns.put("browsername", browsername)
        columns.put("browserversion", browserversion)
        columns.put("osname", osname)
        columns.put("osversion", osversion)


        // HBase API Put
        val rowkey = getRowKey(day, referer + url + ip + ua) // rowkey of HBase
        val rk = Bytes.toBytes(rowkey)

        val list = new ListBuffer[((String, String), KeyValue)]()

        for ((k, v) <- columns) {
          val keyValue = new KeyValue(rk, "info".getBytes(), Bytes.toBytes(k), Bytes.toBytes(v))
          list += (rowkey, k) -> keyValue
        }
        list.toList
      })
    }).sortByKey().map(x => (new ImmutableBytesWritable(Bytes.toBytes(x._1._1)), x._2))


    val conf = new Configuration()
    conf.set("hbase.rootdir", "hdfs://localhost:9000/hbase")
    conf.set("hbase.zookeeper.quorum", "localhost:2181")

    val tableName = createTable(day, conf)

    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    conf.set("fs.defaultFS", "hdfs://localhost:9000")
    // Save data
    val job = NewAPIHadoopJob.getInstance(conf)
    val table = new HTable(conf, tableName)
    HFileOutputFormat2.configureIncrementalLoad(job, table.getTableDescriptor, table.getRegionLocator)


    val output = "hdfs://localhost:9000/etl/access/hbase"
    val outputPath = new Path(output)
    hbaseInfoRDD.saveAsNewAPIHadoopFile(
      output,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      job.getConfiguration
    )

    if (FileSystem.get(conf).exists(outputPath)) {
      val load = new LoadIncrementalHFiles(job.getConfiguration)
      load.doBulkLoad(outputPath, table)

      FileSystem.get(conf).delete(outputPath, true)
    }
    logInfo(s"save successfully... $day")
    spark.stop()
  }

  def getRowKey(time: String, info: String) = {

    val builder = new StringBuilder(time)
    builder.append("_")

    val crc32 = new CRC32()
    crc32.reset()
    if (StringUtils.isNotEmpty(info)) {
      crc32.update(Bytes.toBytes(info))
    }
    builder.append(crc32.getValue)

    builder.toString()
  }

  def createTable(day: String, conf: Configuration) = {
    val table = "access_V3_" + day

    var connection: Connection = null
    var admin: Admin = null
    try {
      connection = ConnectionFactory.createConnection(conf)
      admin = connection.getAdmin()

      val tableName = TableName.valueOf(table)
      // This Spark job is scheduled to run offline once a day.
      // If there are any issues during the processing,
      // the next time it is rerun, it clears the table data first before rewriting it.
      if (admin.tableExists(tableName)) {
        admin.disableTable(tableName)
        admin.deleteTable(tableName)
      }
      val tableDesc = new HTableDescriptor(TableName.valueOf(table))
      val columnDesc = new HColumnDescriptor("info")
      tableDesc.addFamily(columnDesc)
      admin.createTable(tableDesc)

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (null != admin) {
        admin.close()
      }

      if (null != connection) {
        connection.close()
      }
    }

    table
  }
}

