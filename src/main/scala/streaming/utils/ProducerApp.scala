package streaming.utils

import java.util
import java.util.{Date, Properties, UUID}
import com.alibaba.fastjson.JSONObject
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.util.Random

/**
 * Kafka data producer
 */
object ProducerApp {
  def main(args: Array[String]): Unit = {
    val props = new Properties
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("bootstrap.servers", ParamsConf.brokers)
    props.put("request.required.acks", "1")

    val topic = ParamsConf.topic
    val producer = new KafkaProducer[String, String](props)

    val random = new Random()
    val dateFormat = FastDateFormat.getInstance("yyyyMMddHHmmss")

    for (i <- 1 to 100) {
      val time = dateFormat.format(new Date()) + ""
      val userid = random.nextInt(1000) + ""
      val courseid = random.nextInt(500) + ""
      val fee = random.nextInt(400) + ""
      val result = Array("0", "1") // 0: Unsuccessful payment, 1: Successful payment
      val flag = result(random.nextInt(2))
      val orderid = UUID.randomUUID().toString

      val map = new util.HashMap[String, Object]()
      map.put("time", time)
      map.put("userid", userid)
      map.put("courseid", courseid)
      map.put("fee", fee)
      map.put("flag", flag)
      map.put("orderid", orderid)

      val json = new JSONObject(map)

      val futureResult = producer.send(new ProducerRecord[String, String](topic(0), json + ""))
      try {
        val metadata = futureResult.get
        println("Message sent successfully:")
        println("Topic: " + metadata.topic)
        println("Partition: " + metadata.partition)
        println("Offset: " + metadata.offset)
      } catch {
        case e@(_: InterruptedException) =>
          println("Failed to send message: " + e.getMessage)
      }
    }
    println("send data successfully")
  }
}
