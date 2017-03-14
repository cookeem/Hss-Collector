package cmgd.zenghj.hss

import akka.actor.{ActorRef, ActorSystem}
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Source}
import cmgd.zenghj.hss.common.CommonUtils._
import cmgd.zenghj.hss.es.EsUtils.{esBulkProcessor, esBulkRef, esClient, esConn, esConnect, esIndexInit}
import cmgd.zenghj.hss.ftp.FtpUtils
import cmgd.zenghj.hss.kafka.KafkaUtils.kafkaCreateTopic
import cmgd.zenghj.hss.redis.RedisUtils._
import com.twitter.chill.KryoInjection
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * Created by cookeem on 17/3/12.
  */
object AppListFile extends App {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  var redisProbe = redisCheck()
  while (redisProbe != "") {
    consoleLog("ERROR", s"Connect redis error, wait 5 seconds to reconnect!")
    Thread.sleep(5 * 1000)
    redisPool = redisConnect()
    redisProbe = redisCheck()
  }
  consoleLog("SUCCESS", s"Connect redis success")

  //等待kafka连接成功
  var kafkaProbe = kafkaCreateTopic(configKafkaFilesTopic, configKafkaNumPartitions, configKafkaReplication)
  while (kafkaProbe != "") {
    consoleLog("ERROR", s"Connect kafka error, wait 5 seconds to reconnect!")
    Thread.sleep(5 * 1000)
    kafkaProbe = kafkaCreateTopic(configKafkaFilesTopic, configKafkaNumPartitions, configKafkaReplication)
  }
  consoleLog("SUCCESS", s"Connect kafka success")

  //等待elasticsearch连接成功
  var esProbe = esIndexInit()
  while (esProbe != "") {
    consoleLog("ERROR", s"Connect elasticsearch error, wait 5 seconds to reconnect!")
    Thread.sleep(5 * 1000)
    esConn = esConnect()
    esClient = esConn._1
    esBulkProcessor = esConn._2
    esBulkRef = esConn._3
    esProbe = esIndexInit()
  }
  consoleLog("SUCCESS", s"Connect elasticsearch success")

  val kafkaActor = filenameToKafkaActor()
  var countDirFiles = 0

  //从redis中获取还没有完成处理的文件清单,重新写回kafka
  var neType = 0
  redisProcessingFilesGet(neType).foreach { case (dir, filename) =>
    kafkaActor ! (dir, filename)
    countDirFiles += 1
  }
  if (countDirFiles > 0) {
    consoleLog("INFO", s"filename sink to kafka success, record count [$countDirFiles]")
  }
  redisProcessingFileDeleteAll(neType)

  countDirFiles = 0
  neType = 1
  redisProcessingFilesGet(neType).foreach { case (dir, filename) =>
    kafkaActor ! (dir, filename)
    countDirFiles += 1
  }
  if (countDirFiles > 0) {
    consoleLog("INFO", s"filename sink to kafka success, record count [$countDirFiles]")
  }
  redisProcessingFileDeleteAll(neType)

  while (true) {
    //进行目录列表操作
    val futureDirs: Future[Seq[String]] = listDirLv3(configFtpRemoteRoot)
    val currentDirs: Seq[String] = Await.result(futureDirs, Duration.Inf)
    currentDirs.foreach { dir =>
      //进行目录文件列表操作,并且写入redis
      val newFiles: Seq[String] = listFiles(dir)
      //把新的文件名写入到kafka中
      var countDirFiles = 0
      newFiles.foreach { filename =>
        kafkaActor ! (dir, filename)
        countDirFiles += 1
      }
      if (countDirFiles > 0) {
        consoleLog("INFO", s"filename sink to kafka success, record count [$countDirFiles]")
      }
    }
    Thread.sleep(configMasterScheduleInterval * 1000)
  }


  //从ftp上获取最新的dir列表，列表深度3
  def listDirLv3(rootDir: String): Future[Seq[String]] = {
    var futureDirs = Future(Seq[String]())
    try {
      val ftpUtils = FtpUtils(configFtpHost, configFtpPort, configFtpUser, configFtpPass)
      val startTime = System.currentTimeMillis()
      val futureSeqMixDir: Future[Seq[Future[Seq[Future[Seq[String]]]]]] = Future {
        val ftpLv1 = ftpUtils.ls(rootDir, 1).map { ftpFile =>
          s"$rootDir/${ftpFile.getName}"
        }
        if (ftpLv1.isEmpty) {
          Seq(Future(Seq(Future(Seq(rootDir)))))
        } else {
          ftpLv1.map { dirLv2 =>
            Future {
              val ftpLv2: Seq[String] = ftpUtils.ls(dirLv2, 1).map { ftpFile =>
                s"$dirLv2/${ftpFile.getName}"
              }
              if (ftpLv2.isEmpty) {
                Seq(Future(Seq(dirLv2)))
              } else {
                ftpLv2.map { dirLv3 =>
                  Future {
                    val ftpLv3: Seq[String] = ftpUtils.ls(dirLv3, 1).map { ftpFile =>
                      s"$dirLv3/${ftpFile.getName}"
                    }
                    if (ftpLv3.isEmpty) {
                      Seq(dirLv3)
                    } else {
                      ftpLv3
                    }
                  }
                }
              }
            }
          }
        }
      }
      futureDirs = futureSeqMixDir
        .map(Future.sequence(_)).flatMap(t => t).map(_.flatten)
        .map(Future.sequence(_)).flatMap(t => t).map(_.flatten)
      futureDirs.foreach { seq =>
        val endTime = System.currentTimeMillis()
        val duration = endTime - startTime
        consoleLog("INFO", s"ftp ls duration: $duration ms, total dir count: [${seq.length}]")
      }
    } catch { case e: Throwable =>
      consoleLog("ERROR", s"list dir error: rootDir = $rootDir. ${e.getClass}, ${e.getMessage}, ${e.getCause}")
    }
    futureDirs
  }

  //进行文件列表工作,并且返回新的文件列表, Array[filename], 不包含dir
  def listFiles(dir: String): Seq[String] = {
    var ret = Seq[String]()
    try {
      val ftpUtils = FtpUtils(configFtpHost, configFtpPort, configFtpUser, configFtpPass)
      val filesArr: Seq[String] = ftpUtils.ls(dir, 2).map(_.getName).sorted
      ret = redisFilesInsert(dir, filesArr)
    } catch { case e: Throwable =>
      consoleLog("ERROR", s"list file error: dir = $dir. ${e.getClass}, ${e.getMessage}, ${e.getCause}")
    }
    ret
  }

  //暴露一个ActorRef，用于接收(dir, filename)，并写入kafka
  def filenameToKafkaActor()(implicit system: ActorSystem, materializer: ActorMaterializer): ActorRef = {
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

}
