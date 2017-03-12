package cmgd.zenghj.hss.actor

import java.io.{File, FileInputStream, FileOutputStream}
import java.util.zip.GZIPInputStream

import akka.actor.ActorRef
import cmgd.zenghj.hss.common.CommonUtils._
import akka.cluster.ClusterEvent.{MemberRemoved, MemberUp}
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import cmgd.zenghj.hss.es.EsUtils._
import cmgd.zenghj.hss.ftp.FtpUtils
import cmgd.zenghj.hss.redis.RedisUtils._
import com.twitter.chill.KryoInjection
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * Created by cookeem on 16/7/31.
  */

class GetFileWorker extends  TraitClusterActor {
  implicit val system = context.system
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  //当前处理的文件数
  var fileCount = 0
  //当前失败的文件数
  var fileFailCount = 0
  //当前处理的记录数
  var recordCount = 0

  kafkaFtpToEs()

  //从kafka读取文件名，并ftp下载文件，然后把文件内容入库es
  def kafkaFtpToEs() = {
    val consumerSettings = ConsumerSettings(system, new StringDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(configKafkaBrokers)
      .withGroupId(configKafkaConsumeGroup)
      .withWakeupTimeout(30.seconds)
      .withProperty("session.timeout.ms", "30000")

    Consumer.committableSource(consumerSettings, Subscriptions.topics(configKafkaFilesTopic)).map { kafkaMsg =>
      val tryObj = KryoInjection.invert(kafkaMsg.record.value())
      tryObj match {
        case Success(obj) =>
          val tryObj2 = Try(obj.asInstanceOf[(String, String)])
          tryObj2 match {
            case Success((dir, filename)) =>
              ftpFileToEs(dir, filename, self)
            case Failure(e) =>
              fileFailCount += 1
              consoleLog("ERROR", s"kafka consume files topic serialize error: ${e.getClass}, ${e.getMessage}, ${e.getCause}")
          }
        case Failure(e) =>
          fileFailCount += 1
          consoleLog("ERROR", s"kafka consume files topic serialize error: ${e.getClass}, ${e.getMessage}, ${e.getCause}")
      }
    }.runWith(Sink.ignore)
  }

  def receive: Receive =
    eventReceive.orElse {
      case MemberUp(member) =>
        log.info(s"Member is Up: ${member.address}")
      case MemberRemoved(member, previousStatus) =>
        log.warning(s"Member is Removed: ${member.address} after $previousStatus")
      case DirectiveStat =>
        sender() ! DirectiveStatResult(fileCount, fileFailCount, recordCount)
        fileCount = 0
        recordCount = 0
        fileFailCount = 0
      case DirectiveRecordCount(retDir, retFilename, retFilenameTxt, retRecordCount) =>
        if (retRecordCount == -1) {
          fileFailCount += 1
        } else {
          recordCount += retRecordCount
        }
        //只要完成处理,就把redis上的hss:processingfiles记录删除
        redisProcessingFileRemove(retDir, retFilename)
        fileCount += 1
        //删除解压的文件
        val fileTxt = new File(retFilenameTxt)
        if (fileTxt.exists()) {
          fileTxt.delete()
        }
      case e =>
        log.error(s"Unhandled message: ${e.getClass} : $e ")
    }

  //对ftp的日志进行解压，成功返回解压的文件名
  def gzExtract(filename: String): String = {
    var filenameExtract = s"$filename.txt"
    try {
      val gzis = new GZIPInputStream(new FileInputStream(filename))
      val out = new FileOutputStream(filenameExtract)
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

  //进行ftp文件下载,并把文件写入到es
  def ftpFileToEs(dir: String, filename: String, ref: ActorRef) = {
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
    val ftpGetSuccess = ftpUtils.get(localPath, filename, remotePath, filename)
    val filenameGz = s"$localPath/$filename"
    if (ftpGetSuccess) {
      val filenameTxt = gzExtract(filenameGz)
      if (filenameTxt == "") {
        fileFailCount += 1
      } else {
        if (dir.indexOf("_HW/") > 0) {
          esBulkInsertHuawei(filenameTxt, dir, filename, ref)
        } else {
          esBulkInsertEric(filenameTxt, dir, filename, ref)
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
  }

}
