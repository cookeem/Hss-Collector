package cmgd.zenghj.hss.kafka

import cmgd.zenghj.hss.common.CommonUtils._
import java.util.Properties

import akka.actor.{ActorRef, ActorSystem}
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Source}
import com.twitter.chill.KryoInjection
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

/**
  * Created by cookeem on 16/7/26.
  */
object KafkaUtils {

  //暴露一个ActorRef，用于接收(dir, filename)，并写入kafka
  def filenameToKafka()(implicit system: ActorSystem, materializer: ActorMaterializer): ActorRef = {
    val producerSettings = ProducerSettings(system, new StringSerializer, new ByteArraySerializer)
      .withBootstrapServers(configKafkaBrokers)

    //把source暴露成一个actorRef,用于接收消息
    val sourceDirFiles = Source.actorRef[(String, String)](Int.MaxValue, OverflowStrategy.fail)
    Flow[ProducerRecord[String, Array[Byte]]].to(
      Producer.plainSink(producerSettings)
    ).runWith(
      sourceDirFiles.map{ case (dir, filename) =>
        KryoInjection((dir, filename))
      }.map { elem =>
        new ProducerRecord[String, Array[Byte]](configKafkaFilesTopic, elem)
      }
    )
  }


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
