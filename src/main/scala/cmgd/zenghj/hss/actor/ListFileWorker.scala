package cmgd.zenghj.hss.actor

import cmgd.zenghj.hss.common.CommonUtils._
import akka.cluster.ClusterEvent.{MemberRemoved, MemberUp}
import akka.stream.ActorMaterializer
import cmgd.zenghj.hss.ftp.FtpUtils
import cmgd.zenghj.hss.kafka.KafkaUtils.filenameToKafka
import cmgd.zenghj.hss.redis.RedisUtils.redisFilesInsert

import scala.concurrent.Future

/**
  * Created by cookeem on 16/7/28.
  */

//对特定目录进行文件列表，并且把不重复的文件写入到kafka
class ListFileWorker extends TraitClusterActor {
  implicit val system = context.system
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  val kafkaActor = filenameToKafka()

  def receive: Receive =
    eventReceive.orElse {
      case MemberUp(member) =>
        log.info(s"Member is Up: ${member.address}")
      case MemberRemoved(member, previousStatus) =>
        log.error(s"Member is Removed: ${member.address} after $previousStatus")
      case DirectiveListFile(dir) =>
        //进行目录文件列表工作,并且写入redis
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
      case e =>
        log.error(s"Unhandled message: ${e.getClass} : $e ")
    }

  //进行文件列表工作,并且返回新的文件列表, Array[filename], 不包含dir
  def listFiles(dir: String): Seq[String] = {
    val ftpUtils = FtpUtils(configFtpHost, configFtpPort, configFtpUser, configFtpPass)
    val filesArr: Seq[String] = ftpUtils.ls(dir, 2).map(_.getName).sorted
    val newFileArr = redisFilesInsert(dir, filesArr)
    newFileArr
  }

}
