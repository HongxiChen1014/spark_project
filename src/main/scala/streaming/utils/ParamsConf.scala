package streaming.utils

import com.typesafe.config.ConfigFactory

/**
 * Class for reading project parameter configurations
 *
 * Unified configuration management
 */
object ParamsConf {
  private lazy val config = ConfigFactory.load()
  val topic = config.getString("kafka.topic").split(",")
  val groupId = config.getString("kafka.group.id")
  val brokers = config.getString("kafka.broker.list")

  val redisHost = config.getString("redis.host")
  val redisDB = config.getInt("redis.db")

  def main(args: Array[String]): Unit = {
    println(ParamsConf.topic)
    println(ParamsConf.groupId)
    println(ParamsConf.brokers)

    println(ParamsConf.redisHost)
    println(ParamsConf.redisDB)
  }
}
