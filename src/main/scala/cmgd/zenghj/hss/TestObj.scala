package cmgd.zenghj.hss

import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Producer
import akka.kafka.ProducerSettings
import akka.stream.scaladsl.{Flow, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import cmgd.zenghj.hss.common.CommonUtils._
import cmgd.zenghj.hss.kafka.KafkaUtils.kafkaCreateTopic
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

/**
  * Created by cookeem on 17/3/12.
  */
object TestObj extends App {

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  val topicName = "orz-topic"
  val consumerGroup = "test"
  kafkaCreateTopic(topicName, 12, 1)

  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers(configKafkaBrokers)

  //把source暴露成一个actorRef,用于接收消息
  val source = Source.actorRef[String](Int.MaxValue, OverflowStrategy.fail)
  val ref = Flow[ProducerRecord[String, String]].to(
    Producer.plainSink(producerSettings)
  ).runWith(
    source.map { elem =>
      new ProducerRecord[String, String](topicName, elem)
    }
  )

  var i = 0
  while (true) {
    i += 1
    val dNow = new Date()
    val ft = new SimpleDateFormat ("E yyyy.MM.dd 'at' hh:mm:ss a zzz")
    ref ! s"count [$i], time is ${ft.format(dNow)}"
    println(s"send: count [$i], time is ${ft.format(dNow)}")
    Thread.sleep(3 * 1000)
  }

}
