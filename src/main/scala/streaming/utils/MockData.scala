package streaming.utils

import java.util
import java.util.{Date, Map, UUID}
import com.alibaba.fastjson.JSONObject
import scala.util.Random
import org.apache.commons.lang3.time.FastDateFormat

/**
 * Fee Log Generator
 */
object MockData {
  def main(args: Array[String]): Unit = {
    val random = new Random()
    val dateFormat = FastDateFormat.getInstance("yyyyMMddHHmmss")

    //time,userid,courseid,orderid,fee,flag
    for (i <- 0 to 9) { // Scala
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
      println(json)
    }
  }

}
