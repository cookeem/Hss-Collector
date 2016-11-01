package cmgd.zenghj.hss.kafka

import cmgd.zenghj.hss.common.CommonUtils._
import java.util.Properties

import com.twitter.chill.KryoInjection
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.JavaConversions._

/**
  * Created by cookeem on 16/7/26.
  */
object KafkaUtils {
  //初始化生成kafkaFilesTopic
  kafkaCreateTopic(configKafkaZkUri, configKafkaFilesTopic, configKafkaNumPartitions, configKafkaReplication)

  //把新的文件名写入kafka,记录格式为Tuple2[dir, filename]
  def filenameSinkKafka(newDirFiles: Array[(String, String)]) = {
    val startTime = System.currentTimeMillis()
    try {
      val props = new Properties()
      props.put("bootstrap.servers", configKafkaBrokers)
      props.put("acks", "all")
      props.put("retries", 0.toString)
      props.put("batch.size", 1024.toString)
      props.put("linger.ms", 1.toString)
      //        props.put("buffer.memory", 33554432)
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
      val producer = new KafkaProducer[String, Array[Byte]](props)
      newDirFiles.foreach { record =>
        val bytes = KryoInjection(record)
        producer.send(new ProducerRecord[String, Array[Byte]](configKafkaFilesTopic, bytes))
      }
      producer.close()
      val duration = Math.round(System.currentTimeMillis() - startTime)
      consoleLog("SUCCESS", s"file name sink to kafka success # took $duration ms")
    } catch {
      case e: Throwable =>
        val duration = Math.round(System.currentTimeMillis() - startTime)
        consoleLog("ERROR", s"file name sink to kafka error, ${e.getMessage}, ${e.getCause} # took $duration ms")
    }
  }

  def kafkaCreateTopic(zkUri: String, topic: String, numPartitions: Int, replicationFactor: Int): Boolean = {
    val sessionTimeoutMs = 10000
    val connectionTimeoutMs = 10000
    var success = false
    try {
      //注意,谨慎使用,AdminUtils连接经常会出现超时,切勿频繁使用
      val (zkClient, zkConnection) = ZkUtils.createZkClientAndConnection(zkUri, sessionTimeoutMs, connectionTimeoutMs)
      val zkUtils = new ZkUtils(zkClient, zkConnection, false)
      val topicConfig = new Properties()
      if (!AdminUtils.topicExists(zkUtils, topic)) {
        AdminUtils.createTopic(zkUtils, topic, numPartitions, replicationFactor, topicConfig)
        consoleLog("SUCCESS", s"create kafka topic success: zkUri = $zkUri, topic = $topic, numPartitions = $numPartitions, replicationFactor = $replicationFactor.")
      }
      zkUtils.close()
      zkClient.close()
      zkConnection.close()
      success = true
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"create kafka topic error: zkUri = $zkUri, topic = $topic, numPartitions = $numPartitions, replicationFactor = $replicationFactor. ${e.getMessage}, ${e.getCause}")
    }
    success
  }


}
