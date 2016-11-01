package cmgd.zenghj.hss.actor

import java.io.File

import akka.actor.{PoisonPill, ActorLogging, Actor}
import cmgd.zenghj.hss.common.CommonUtils._
import cmgd.zenghj.hss.ftp.FtpUtils
import cmgd.zenghj.hss.es.EsUtils._
import cmgd.zenghj.hss.redis.RedisUtils._

/**
  * Created by cookeem on 16/8/10.
  */
class FtpKafkaWorker  extends Actor with ActorLogging {
  def receive = {
    case DirectiveStopMember =>
      self ! PoisonPill
    case DirectiveFtpKafka(dir, filename) =>
      //进行ftp文件下载,并把文件写入到kafka
      val ftpUtils = FtpUtils(configFtpHost, configFtpPort, configFtpUser, configFtpPass)
      val file = new File(s"$configFtpLocalRoot/$dir")
      if (!file.exists()) {
        file.mkdirs()
      }
      val localPath = file.getAbsolutePath
      val remotePath = s"$configFtpRoot/$dir"
      val ftpGetSuccess = ftpUtils.get(localPath, filename, remotePath, filename)
      var recordCount = 0
      var fileFailCount = 0
      if (ftpGetSuccess) {
        val absFilename = s"$localPath/$filename"
        recordCount = bulkInsert(absFilename)
      } else {
        fileFailCount = 1
      }
      //只要成功处理,就把redis上的hss:processingfiles记录删除
      redisProcessingFileRemove(dir, filename)
      sender() ! DirectiveFtpKafkaResult(fileFailCount, recordCount)
    case e =>
      log.error(s"Unhandled message: ${e.getClass} : $e ")
  }

}
