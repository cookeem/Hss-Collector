package cmgd.zenghj.hss

import java.io.{BufferedOutputStream, File, FileInputStream, FileOutputStream}
import java.util.zip.GZIPInputStream

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import cmgd.zenghj.hss.common.CommonUtils._
import cmgd.zenghj.hss.es.EsUtils.{esBulkInsertEric, esBulkInsertHuawei, esBulkProcessor, esBulkRef, esClient, esConn, esConnect, esIndexInit}
import cmgd.zenghj.hss.ftp.FtpUtils
import cmgd.zenghj.hss.kafka.KafkaUtils.kafkaCreateTopic
import cmgd.zenghj.hss.redis.RedisUtils._
import com.twitter.chill.KryoInjection
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.concurrent.duration._
import scala.sys.process._
import scala.util.{Failure, Success, Try}

/**
  * Created by cookeem on 17/3/12.
  */
object AppGetFile extends App {
  implicit val system = ActorSystem("hss-getfile")
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

  //当前处理的文件数
  var fileCount = 0
  //当前失败的文件数
  var fileFailCount = 0
  //当前处理的记录数
  var recordCount = 0

  //从kafka读取文件名，并ftp下载文件，然后把文件内容入库es
  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new ByteArrayDeserializer)
    .withBootstrapServers(configKafkaBrokers)
    .withGroupId(configKafkaConsumeGroup)
    .withWakeupTimeout(30.seconds)
    .withProperty("session.timeout.ms", "30000")
    .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

  Consumer.committableSource(consumerSettings, Subscriptions.topics(configKafkaFilesTopic)).map { kafkaMsg =>
    kafkaMsg.committableOffset.commitScaladsl()
    val tryObj = KryoInjection.invert(kafkaMsg.record.value())
    tryObj match {
      case Success(obj) =>
        val tryObj2 = Try(obj.asInstanceOf[(String, String)])
        tryObj2 match {
          case Success((dir, filename)) =>
            ftpFileToEs(dir, filename)
          case Failure(e) =>
            fileFailCount += 1
            consoleLog("ERROR", s"kafka consume files topic serialize error: ${e.getClass}, ${e.getMessage}, ${e.getCause}")
        }
      case Failure(e) =>
        fileFailCount += 1
        consoleLog("ERROR", s"kafka consume files topic serialize error: ${e.getClass}, ${e.getMessage}, ${e.getCause}")
    }
  }.runWith(Sink.ignore)

  consoleLog("INFO", "kafkaFtpToEs started!")

  //对ftp的日志进行解压，成功返回解压的文件名
  def gzExtract(filename: String): String = {
    var filenameExtract = s"$filename.txt"
    try {
      val gzis = new GZIPInputStream(new FileInputStream(filename))
      val out = new BufferedOutputStream(new FileOutputStream(filenameExtract))
      val bytes = new Array[Byte](1024)
      Iterator.continually(gzis.read(bytes)).takeWhile(_ != -1).foreach(read => out.write(bytes, 0, read))
      gzis.close()
      out.close()
    } catch {
      case e: Throwable =>
        filenameExtract = ""
        consoleLog("ERROR", s"gzip extract file error: filename = $filename, ${e.getClass}, ${e.getMessage}, ${e.getCause}")
    }
    filenameExtract
  }

  def shellExtract(absFileName: String) = {
    var ret = ""
    try {
      val arr = absFileName.split("/")
      val filename = arr(arr.length - 1)
      val tmpdir = s"$filename.dir"
      arr(arr.length - 1) = ""
      val path = arr.mkString("/")
      Process(
        Seq("mkdir","-p", tmpdir),
        new File(path)
      ).!!
      Process(
        Seq("tar","zxvf", filename, "-C", tmpdir),
        new File(path)
      ).!!
      val extractFileName = Process(
        Seq("ls", tmpdir),
        new File(path)
      ).!!.replace("\n", "")

      s"grep -E TPLEPSSER|EPSSER $path$tmpdir/$extractFileName" #> new File(s"$path$extractFileName") !

      Process(
        Seq("rm", "-rf", s"$tmpdir"),
      new File(path)
      ).!!
      ret = s"$path$extractFileName"
    } catch { case e: Throwable =>
      consoleLog("ERROR", s"gzip extract file error: filename = $absFileName, ${e.getClass}, ${e.getMessage}, ${e.getCause}")
    }
    ret
  }

  //进行ftp文件下载,并把文件写入到es
  def ftpFileToEs(dir: String, filename: String): Unit = {
    try {
      //只要从kafka成功读取目录名和文件名,就删除redis上的hss:dir:xxx:files记录
      redisFileRemove(dir, filename)
      //只要从kafka成功读取目录名和文件名,插入到redis上的hss:processingfiles记录中
      redisProcessingFileInsert(dir, filename)

      val ftpUtils = FtpUtils(configFtpHost, configFtpPort, configFtpUser, configFtpPass)
      val fileDir = new File(s"$configFtpLocalRoot/$dir")
      if (!fileDir.exists()) {
        fileDir.mkdirs()
      }
      val localPath = fileDir.getAbsolutePath
      val remotePath = dir
      consoleLog("DEBUG", s"begin to get ftp file $filename")
      val ftpGetSuccess = ftpUtils.get(localPath, filename, remotePath, filename)
      val filenameGz = s"$localPath/$filename"
      if (ftpGetSuccess) {
        var filenameTxt = ""
        if (dir.indexOf("_HW/") > 0) {
          filenameTxt = shellExtract(filenameGz)
        } else {
          filenameTxt = gzExtract(filenameGz)
        }
        consoleLog("DEBUG", s"extract $filenameGz success")
        if (filenameTxt == "") {
          fileFailCount += 1
        } else {
          var rc = 0
          if (dir.indexOf("_HW/") > 0) {
            rc = esBulkInsertHuawei(filenameTxt)
          } else {
            rc = esBulkInsertEric(filenameTxt)
          }
          if (rc > -1) {
            //删除解压后的文件
            val fileTxt = new File(filenameTxt)
            if (fileTxt.exists()) {
              fileTxt.delete()
            }
            recordCount += rc
          } else {
            fileFailCount += 1
          }
          //删除压缩的文件
          val fileGz = new File(filenameGz)
          if (fileGz.exists()) {
            fileGz.delete()
          }
        }
      } else {
        fileFailCount += 1
      }
      fileCount += 1

      //只要完成处理,就把redis上的hss:processingfiles记录删除
      redisProcessingFileRemove(dir, filename)
    } catch { case e: Throwable =>
      consoleLog("ERROR", s"ftp file to elasticsearch error: dir = $dir, filename = $filename. ${e.getClass}, ${e.getMessage}, ${e.getCause}")
    }
  }

}
