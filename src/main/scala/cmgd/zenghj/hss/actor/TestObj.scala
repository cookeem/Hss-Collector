package cmgd.zenghj.hss.actor

import akka.Done
import akka.actor.ActorRef
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Flow

import scala.concurrent.Future


/**
  * Created by cookeem on 17/3/9.
  */
object TestObj {
  import akka.actor.ActorSystem
  import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
  import akka.kafka.scaladsl.{Consumer, Producer}
  import akka.stream.ActorMaterializer
  import akka.stream.scaladsl.{Sink, Source}
  import cmgd.zenghj.hss.common.CommonUtils._
  import cmgd.zenghj.hss.kafka.KafkaUtils
  import org.apache.kafka.clients.consumer.ConsumerConfig
  import org.apache.kafka.clients.producer.ProducerRecord
  import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}


  implicit val system = ActorSystem("test")
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  val topicName = "test"
  KafkaUtils.kafkaCreateTopic(topicName, 6, 1)

  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers(configKafkaBrokers)

  //把source暴露成一个actorRef,用于接收消息
  val source: Source[String, ActorRef] = Source.actorRef[String](Int.MaxValue, OverflowStrategy.fail)
  val ref = Flow[ProducerRecord[String, String]].to(
    Producer.plainSink(producerSettings)
  ).runWith(
    source.map{ str =>
      s"string '$str' length is ${str.length}"
    }.map { elem =>
      new ProducerRecord[String, String](topicName, elem)
    }
  )
  ref ! "a"
  ref ! "aa"
  ref ! "aaa"

  Source(1 to 10).map(i => i.toString + ":" + System.currentTimeMillis()).map { elem =>
    new ProducerRecord[String, String](topicName, elem)
  }.runWith(Producer.plainSink(producerSettings))

  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers(configKafkaBrokers)
    .withGroupId(configKafkaConsumeGroup)
//    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  Consumer.committableSource(consumerSettings, Subscriptions.topics(topicName)).map { kafkaMsg =>
    println(kafkaMsg.record.value())
  }.to(Sink.ignore).run()


}
