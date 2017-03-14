package cmgd.zenghj.hss

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import cmgd.zenghj.hss.common.CommonUtils.configKafkaBrokers
import cmgd.zenghj.hss.kafka.KafkaUtils.kafkaCreateTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.duration._

/**
  * Created by cookeem on 17/3/13.
  */
object TestObj2 extends App {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  val topicName = "orz-topic"
  val consumerGroup = "test"
  kafkaCreateTopic(topicName, 12, 1)

  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers(configKafkaBrokers)
    .withGroupId(consumerGroup)
    .withWakeupTimeout(30.seconds)
    .withProperty("session.timeout.ms", "30000")
    .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

  Consumer.committableSource(consumerSettings, Subscriptions.topics(topicName)).map { kafkaMsg =>
    kafkaMsg.committableOffset.commitScaladsl()
    println(s"receive ${kafkaMsg.record.value()}")
  }.runWith(Sink.ignore)

  import sys.process.Process
  import java.io.File
  val p: String = Process(Seq("tar","zxvf", "HSS9860_99_20170105060001_pgw_exp_log.dat"), new File("/Volumes/Share/Download/10.243.170.26/HZ_HW/HSS01-02-LOG/HZHSS01/")).!!

}
