package cmgd.zenghj.hss.actor

import akka.actor.RootActorPath
import akka.cluster.ClusterEvent.{MemberRemoved, MemberUp}
import cmgd.zenghj.hss.common.CommonUtils._
import cmgd.zenghj.hss.ftp.FtpUtils
import cmgd.zenghj.hss.kafka.KafkaUtils._
import cmgd.zenghj.hss.redis.RedisUtils._

import akka.routing.{BroadcastRoutingLogic, Router, RoundRobinRoutingLogic}

/**
  * Created by cookeem on 16/7/28.
  */
class CollectorMaster extends TraitClusterActor {
  var router = Router(RoundRobinRoutingLogic(), Vector[akka.routing.ActorRefRoutee]())
  var workerRouter = Router(BroadcastRoutingLogic(), Vector[akka.routing.ActorRefRoutee]())

  //从redis中获取还没有完成处理的文件清单,重新写回kafka
  val processingFiles: Array[(String, String)] = redisProcessingFilesGet()
  filenameSinkKafka(processingFiles)
  //成功重写回kafka的filename后,删除redis的hss:processingfiles所有记录
  redisProcessingFileDeleteAll()

  def receive: Receive =
    eventReceive.orElse {
      case MemberUp(member) =>
        if (member.hasRole("router")) {
          val ref = context.actorSelection(RootActorPath(member.address) / "user" / "collector-router")
          router = router.addRoutee(ref)
          consoleLog("INFO", "current routers:\n" + router.routees.mkString("\n"))
        }
        if (member.hasRole("worker")) {
          val ref = context.actorSelection(RootActorPath(member.address) / "user" / "getfile-worker")
          workerRouter = workerRouter.addRoutee(ref)
          consoleLog("INFO", "current workers:\n" + workerRouter.routees.mkString("\n"))
        }
        log.info(s"Member is Up: ${member.address}")
      case MemberRemoved(member, previousStatus) =>
        if (member.hasRole("router")) {
          val ref = context.actorSelection(RootActorPath(member.address) / "user" / "collector-router")
          router = router.removeRoutee(ref)
          consoleLog("INFO", "current router:\n" + router.routees.mkString("\n"))
        }
        if (member.hasRole("worker")) {
          val ref = context.actorSelection(RootActorPath(member.address) / "user" / "getfile-worker")
          workerRouter = workerRouter.removeRoutee(ref)
          consoleLog("INFO", "current workers:\n" + workerRouter.routees.mkString("\n"))
        }
        log.warning(s"Member is Removed: ${member.address} after $previousStatus")
      //通过ftp获取dir列表, 通知所有CollectorRouter
      case DirectiveListDir =>
        val currentDirs = listDir()
        currentDirs.foreach { dir =>
          router.route(DirectiveListFile(dir), self)
        }
      //向所有GetFileWorker发送DirectiveStat指令
      case DirectiveStat =>
        workerRouter.route(DirectiveStat, self)
      //接收GetFileWorker发送DirectiveStatResult指令
      case DirectiveStatResult(fileCount, fileFailCount, recordCount) =>
        consoleLog("INFO", s"worker: ${sender().path.address}")
        consoleLog("INFO", s"process [$fileCount] files, [$recordCount] records, fail [$fileFailCount] files in $configMasterStatInterval seconds")
      case e =>
        log.error(s"Unhandled message: ${e.getClass} : $e ")
    }

  val ftpUtils = FtpUtils(configFtpHost, configFtpPort, configFtpUser, configFtpPass)

  //从ftp上获取最新的dir列表
  //返回: Array[dir]
  def listDir(): Array[String] = {
    val dirLv1Arr = ftpUtils.ls(configFtpRoot, 1).map(_.getName)
    val dirs: Array[String] = dirLv1Arr.map{ dirLv1 =>
      val dirLv2Arr = ftpUtils.ls(s"$configFtpRoot/$dirLv1", 1).map(_.getName)
      (dirLv1 , dirLv2Arr)
    }.flatMap{ case (dirLv1 , dirLv2Arr) => dirLv2Arr.map{dirLv2 => s"$dirLv1/$dirLv2"}}
    redisDirsUpdate(dirs)
    dirs
  }
}
