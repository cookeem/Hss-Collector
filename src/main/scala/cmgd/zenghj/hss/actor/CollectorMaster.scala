package cmgd.zenghj.hss.actor

import akka.actor.RootActorPath
import akka.cluster.ClusterEvent.{MemberRemoved, MemberUp}
import cmgd.zenghj.hss.common.CommonUtils._
import cmgd.zenghj.hss.ftp.FtpUtils
import cmgd.zenghj.hss.kafka.KafkaUtils._
import cmgd.zenghj.hss.redis.RedisUtils._
import akka.routing.{BroadcastRoutingLogic, RoundRobinRoutingLogic, Router}
import akka.stream.ActorMaterializer

import scala.concurrent.Future

/**
  * Created by cookeem on 16/7/28.
  */
class CollectorMaster extends TraitClusterActor {
  implicit val system = context.system
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  var listFileWorkerRouter = Router(RoundRobinRoutingLogic(), Vector[akka.routing.ActorRefRoutee]())
  var getFileWorkerRouter = Router(BroadcastRoutingLogic(), Vector[akka.routing.ActorRefRoutee]())

  val kafkaActor = filenameToKafka()
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


  def receive: Receive =
    eventReceive.orElse {
      case MemberUp(member) =>
        if (member.hasRole("listfile-worker")) {
          val ref = context.actorSelection(RootActorPath(member.address) / "user" / "listfile-worker")
          listFileWorkerRouter = listFileWorkerRouter.addRoutee(ref)
          consoleLog("INFO", s"listfile-worker joined: ${ref.pathString}")
        }
        if (member.hasRole("getfile-worker")) {
          val ref = context.actorSelection(RootActorPath(member.address) / "user" / "getfile-worker")
          getFileWorkerRouter = getFileWorkerRouter.addRoutee(ref)
          consoleLog("INFO", s"getfile-worker joined: ${ref.pathString}")
        }
        log.info(s"Member is Up: ${member.address}")
      case MemberRemoved(member, previousStatus) =>
        if (member.hasRole("listfile-worker")) {
          val ref = context.actorSelection(RootActorPath(member.address) / "user" / "listfile-worker")
          listFileWorkerRouter = listFileWorkerRouter.removeRoutee(ref)
          consoleLog("ERROR", s"listfile-worker remove ${ref.pathString}")
        }
        if (member.hasRole("getfile-worker")) {
          val ref = context.actorSelection(RootActorPath(member.address) / "user" / "getfile-worker")
          getFileWorkerRouter = getFileWorkerRouter.removeRoutee(ref)
          consoleLog("ERROR", s"getfile-worker remove: ${ref.pathString}")
        }
        log.warning(s"Member is Removed: ${member.address} after $previousStatus")
      //向所有ListFileWorker发送DirectiveListDir指令，通知ListFileWorker进行文件列表
      case DirectiveListDir =>
        val currentDirs: Future[Seq[String]] = listDirLv3(configFtpRemoteRoot)
        currentDirs.foreach(_.foreach { dir =>
          listFileWorkerRouter.route(DirectiveListFile(dir), self)
        })
      //向所有GetFileWorker发送DirectiveStat指令，通知GetFileWorker获取文件并写入到es
      case DirectiveStat =>
        getFileWorkerRouter.route(DirectiveStat, self)
      //接收GetFileWorker发送DirectiveStatResult指令
      case DirectiveStatResult(fileCount, fileFailCount, recordCount) =>
        consoleLog("INFO", s"worker: ${sender().path.address}")
        consoleLog("INFO", s"process [$fileCount] files, [$recordCount] records, fail [$fileFailCount] files in $configMasterStatInterval seconds")
      case e =>
        log.error(s"Unhandled message: ${e.getClass} : $e ")
    }

  val ftpUtils = FtpUtils(configFtpHost, configFtpPort, configFtpUser, configFtpPass)

  //从ftp上获取最新的dir列表，列表深度3
  def listDirLv3(rootDir: String): Future[Seq[String]] = {
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
    val futureDirs: Future[Seq[String]] = futureSeqMixDir
      .map(Future.sequence(_)).flatMap(t => t).map(_.flatten)
      .map(Future.sequence(_)).flatMap(t => t).map(_.flatten)
    futureDirs.foreach { seq =>
      val endTime = System.currentTimeMillis()
      val duration = endTime - startTime
      consoleLog("INFO", s"ftp ls duration: $duration ms, total dir count: [${seq.length}]")
    }
    futureDirs
  }

}
