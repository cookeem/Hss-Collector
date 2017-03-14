package cmgd.zenghj.hss.kafka

import cmgd.zenghj.hss.common.CommonUtils._
import java.util.Properties

import kafka.admin.AdminUtils
import kafka.utils.ZkUtils

/**
  * Created by cookeem on 16/7/26.
  */
object KafkaUtils {
  def kafkaCreateTopic(topic: String, numPartitions: Int, replicationFactor: Int): String = {
    val sessionTimeoutMs = 10000
    val connectionTimeoutMs = 10000
    var errmsg = ""
    try {
      //注意,谨慎使用,AdminUtils连接经常会出现超时,切勿频繁使用
      val (zkClient, zkConnection) = ZkUtils.createZkClientAndConnection(configKafkaZkUri, sessionTimeoutMs, connectionTimeoutMs)
      val zkUtils = new ZkUtils(zkClient, zkConnection, false)
      val topicConfig = new Properties()
      if (!AdminUtils.topicExists(zkUtils, topic)) {
        AdminUtils.createTopic(zkUtils, topic, numPartitions, replicationFactor, topicConfig)
        consoleLog("SUCCESS", s"create kafka topic success: topic = $topic, numPartitions = $numPartitions, replicationFactor = $replicationFactor.")
      } else {
        consoleLog("INFO", s"kafka topic already exists: topic = $topic")
      }
      zkUtils.close()
      zkClient.close()
      zkConnection.close()
    } catch {
      case e: Throwable =>
        errmsg = s"create kafka topic error: topic = $topic, numPartitions = $numPartitions, replicationFactor = $replicationFactor. ${e.getClass}, ${e.getMessage}, ${e.getCause}"
        consoleLog("ERROR", errmsg)
    }
    errmsg
  }

}
