package batch
import java.util.zip.CRC32
import java.util.{Date, Locale}

import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
object LogApp extends Logging {

  def main(args: Array[String]): Unit = {


    if(args.length != 1) {
      println("Usage: ImoocLogApp <time>")
      System.exit(1)
    }

    //val day = "20190130" //先写死，后续通过shell脚本传递给spark-submit过来
    val day = args(0)

    //val input = "/Users/rocky/IdeaProjects/imooc-workspace/spark-project-train/src/data/test-access.log"
    val input = s"hdfs://hadoop000:8020/access/$day/*"

    //val spark = SparkSession.builder().config("spark.serializer","org.apache.spark.serializer.KryoSerializer").appName("ImoocLogApp").master("local[2]").getOrCreate()

    val spark = SparkSession.builder().getOrCreate()
    var logDF = spark.read.format("com.imooc.bigdata.spark.pk").option("path",input)
      .load()


    // UDF函数的写法
    import org.apache.spark.sql.functions._
    def formatTime() = udf((time:String) =>{
      FastDateFormat.getInstance("yyyyMMddHHmm").format(
        new Date(FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z",Locale.ENGLISH)
          .parse(time.substring(time.indexOf("[")+1, time.lastIndexOf("]"))).getTime
        ))
    })

    logDF = logDF.withColumn("formattime", formatTime()(logDF("time")))

    // ------以上部分已经将我们所需要处理的日志信息进行了处理(ip/ua/time)--------

    //TODO... 数据清洗完了，下一步应该是将数据落地到HBase中（哪些字段属于哪个cf、表名、rowkey）

    val hbaseInfoRDD = logDF.rdd.map(x => {
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

      val columns = scala.collection.mutable.HashMap[String,String]()
      columns.put("ip",ip)
      columns.put("country",country)
      columns.put("province",province)
      columns.put("city",city)
      columns.put("formattime",formattime)
      columns.put("method",method)
      columns.put("url",url)
      columns.put("protocal",protocal)
      columns.put("status",status)
      columns.put("bytessent",bytessent)
      columns.put("referer",referer)
      columns.put("browsername",browsername)
      columns.put("browserversion",browserversion)
      columns.put("osname",osname)
      columns.put("osversion",osversion)


      // HBase API  Put

      val rowkey = getRowKey(day, referer+url+ip+ua)  // HBase的rowkey
      val put = new Put(Bytes.toBytes(rowkey)) // 要保存到HBase的Put对象

      // 每一个rowkey对应的cf中的所有column字段
      for((k,v) <- columns) {
        put.addColumn(Bytes.toBytes("o"), Bytes.toBytes(k.toString), Bytes.toBytes(v.toString));
      }

      (new ImmutableBytesWritable(rowkey.getBytes), put)
    }) //.collect().foreach(println)


    val conf = new Configuration()
    conf.set("hbase.rootdir","hdfs://hadoop000:8020/hbase")
    conf.set("hbase.zookeeper.quorum","hadoop000:2181")

    val tableName = createTable(day, conf)

    // 设置写数据到哪个表中
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    // 保存数据
    hbaseInfoRDD.saveAsNewAPIHadoopFile(
      "hdfs://hadoop000:8020/etl/access/hbase",
      classOf[ImmutableBytesWritable],
      classOf[Put],
      classOf[TableOutputFormat[ImmutableBytesWritable]],
      conf
    )

    logInfo(s"作业执行成功... $day")

    spark.stop()
  }

  def getRowKey(time:String, info:String) = {

    /**
     * 由于rowkey是采用time_crc32(info)进行拼接
     * 只要是字符串拼接，尽量不要使用+  TODO... 是一个非常经典的面试题(Java/Bigdata)
     *
     * StringBuffer vs StringBuilder
     */

    val builder = new StringBuilder(time)
    builder.append("_")

    val crc32 = new CRC32()
    crc32.reset()
    if(StringUtils.isNotEmpty(info)){
      crc32.update(Bytes.toBytes(info))
    }
    builder.append(crc32.getValue)

    builder.toString()
  }

  def createTable(day:String, conf:Configuration) ={
    val table = "access_" + day

    var connection:Connection = null
    var admin:Admin = null
    try {
      connection = ConnectionFactory.createConnection(conf)
      admin = connection.getAdmin

      /**
       * 这个Spark作业是离线的，然后一天运行一次，如果中间处理过程中有问题
       * 下次重跑的时候，是不是应该先把表数据清空，然后重新写入
       */
      val tableName = TableName.valueOf(table)
      if(admin.tableExists(tableName)) {
        admin.disableTable(tableName)
        admin.deleteTable(tableName)
      }

      val tableDesc = new HTableDescriptor(TableName.valueOf(table))
      val columnDesc = new HColumnDescriptor("o")
      tableDesc.addFamily(columnDesc)
      admin.createTable(tableDesc)
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      if(null != admin) {
        admin.close()
      }

      if(null != connection) {
        connection.close()
      }
    }

    table
  }
}

