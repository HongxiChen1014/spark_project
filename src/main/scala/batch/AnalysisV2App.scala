package batch

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import java.sql.DriverManager
import java.util.Base64
import scala.util.{Failure, Success, Try}

/**
 * Author: Daisy Chen
 * Perform statistical analysis operations on data in HBase using Spark
 *
 * 1） Calculate the visit count for each province in each country - Top 10
 * 2） Calculate the visit count for different browsers
 */
object AnalysisV2App extends Logging {
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

    // Calculate the visit count for different browsers
    val resultRDD = hbaseRDD.map(x => {
      val browsername = Bytes.toString(x._2.getValue(cf.getBytes, "browsername".getBytes))
      (browsername, 1)
    }).reduceByKey(_ + _)

    resultRDD.collect().foreach(println)

    resultRDD.coalesce(1).foreachPartition(part => {
      Try {
        // Write the statistical results to MySQL
        val connection = {
          Class.forName("com.mysql.jdbc.Driver")
          val url = "jdbc:mysql://localhost:3306/spark?characterEncoding=UTF-8&useSSL=false"
          val user = "root"
          val password = null
          DriverManager.getConnection(url, user, password)
        }
        val preAutoCommit = connection.getAutoCommit
        connection.setAutoCommit(false)

        val sql = "insert into browser_stat (day,browser,cnt) values(?,?,?)"
        val pstmt = connection.prepareStatement(sql)

        pstmt.addBatch(s"delete from browser_stat where day=$day")

        part.foreach(x => {
          pstmt.setString(1, day)
          pstmt.setString(2, x._1)
          pstmt.setInt(3, x._2)

          pstmt.addBatch()
        })

        pstmt.executeBatch()
        connection.commit()

        (connection, preAutoCommit)
      } match {
        case Success((connection, preAutoCommit)) => {
          connection.setAutoCommit(preAutoCommit)
          if (null != connection) connection.close()
        }
        case Failure(e) => throw e
      }
    })

    spark.stop()

  }


  case class CountryProvince(country: String, province: String)

  case class Browser(browsername: String)
}
