package streaming.spark

import com.alibaba.fastjson.JSON
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import streaming.utils.{ParamsConf, RedisPool}

import scala.collection.JavaConverters.mapAsScalaMapConverter

/**
 * Spark Streaming processing Kafka data
 * Calculate the total number of paid orders and the total amount of orders per day
 * Calculate the total number of paid orders and the total amount of orders per hour
 * Calculate the total number of paid orders and the total amount of orders per minute
 */
object StreamingV2App {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StreamingApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // Get offset from jedis
    val jedis = RedisPool.getJedis()
    val fromOffsets: Map[TopicPartition, Long] = jedis.hgetAll("kafka-offsets").asScala.map { case (key, value) =>
      val Array(topic, partition) = key.split("-")
      new TopicPartition(topic, partition.toInt) -> value.toLong
    }.toMap


    val stream = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](ParamsConf.topic, ParamsConf.kafkaParams, fromOffsets)
    )

    //    stream.map(x => x.value()).print()

    stream.foreachRDD(rdd => {
      // What we need is: flag, fee, time
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val data = rdd.map(x => JSON.parseObject(x.value()))
        .map(x => {
          val flag = x.getString("flag")
          val fee = x.getLong("fee")
          val time = x.getString("time")

          val day = time.substring(0, 8)
          val hour = time.substring(8, 10)
          val minute = time.substring(10, 12)

          val success: (Long, Long) = if (flag == "1") (1, fee) else (0, 0)

          /**
           * Organize the above data into a data structure
           * day, hour, minute: each represents a granularity
           *
           *
           */
          (day, hour, minute, List[Long](1, success._1, success._2))
        })

      // day
      data.map(x => (x._1, x._4))
        .reduceByKey((a, b) => {
          a.zip(b).map(x => x._1 + x._2)
        }).foreachPartition(partition => {
          val jedis = RedisPool.getJedis()
          partition.foreach(x => {
            jedis.hincrBy("Imooc-" + x._1, "total", x._2(0))
            jedis.hincrBy("Imooc-" + x._1, "success", x._2(1))
            jedis.hincrBy("Imooc-" + x._1, "fee", x._2(2))
          })
        })

      // hour
      data.map(x => ((x._1, x._2), x._4))
        .reduceByKey((a, b) => {
          a.zip(b).map(x => x._1 + x._2)
        }).foreachPartition(partition => {
          val jedis = RedisPool.getJedis()
          partition.foreach(x => {
            jedis.hincrBy("Imooc-" + x._1._1, "total" + x._1._2, x._2(0))
            jedis.hincrBy("Imooc-" + x._1._1, "success" + x._1._2, x._2(1))
            jedis.hincrBy("Imooc-" + x._1._1, "fee" + x._1._2, x._2(2))
          })
        })
      val jedis = RedisPool.getJedis()
      try {
        offsetRanges.foreach { offsetRange =>
          val topic = offsetRange.topic
          val partition = offsetRange.partition
          val offset = offsetRange.untilOffset
          jedis.hset("kafka-offsets", s"$topic-$partition", offset.toString)
        }
      } finally {
        jedis.close()
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
