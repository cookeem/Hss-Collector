package cmgd.zenghj.hss

import cmgd.zenghj.hss.actor._
import cmgd.zenghj.hss.common.CommonUtils._

import akka.actor.{Props, ActorSystem}
import com.typesafe.config.ConfigFactory
import org.apache.commons.cli.{Option => CliOption, HelpFormatter, Options, DefaultParser}

import scala.collection.JavaConversions._
import scala.concurrent.duration._

/**
  * Created by cookeem on 16/7/28.
  */
object HssCollector extends App {
  val options = new Options()
  //单参数选项
  options.addOption(CliOption.builder("s").longOpt("service")
    .desc("Service name: master or router or worker")
    .hasArg()
    .required()
    .argName("SERVICE")
    .build())
  try {
    val parser = new DefaultParser()
    val cmd = parser.parse(options, args)
    val service = cmd.getOptionValue("s")
    if (service != "master" && service != "router" && service != "worker") {
      throw new Throwable()
    }
    //启动处理逻辑
    service match {
      //启动CollectorMaster
      case "master" =>
        //启动单个master，端口2551
        val serviceConfigMaster = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=2551")
          .withFallback(ConfigFactory.parseString("akka.cluster.auto-down-unreachable-after = 5s"))
          .withFallback(ConfigFactory.parseString("akka.cluster.roles = [master]"))
          .withFallback(config)
        val systemMaster = ActorSystem("hss-cluster", serviceConfigMaster)
        val collectorMasterRef = systemMaster.actorOf(Props[CollectorMaster], "collector-master")
        import systemMaster.dispatcher
        //启动定期任务, 通知CollectorRouter进行文件夹列表任务
        systemMaster.scheduler.schedule(configMasterScheduleInterval.seconds, configMasterScheduleInterval.seconds) {
          collectorMasterRef ! DirectiveListDir
        }
        //启动定期任务, 通知GetFileWorker收集执行结果信息
        systemMaster.scheduler.schedule(configMasterStatInterval.seconds, configMasterStatInterval.seconds) {
          collectorMasterRef ! DirectiveStat
        }

        //启动单个router，端口2552
        val serviceConfigRouter = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=2552")
          .withFallback(ConfigFactory.parseString("akka.cluster.auto-down-unreachable-after = 5s"))
          .withFallback(ConfigFactory.parseString("akka.cluster.roles = [router]"))
          .withFallback(config)
        val systemRouter = ActorSystem("hss-cluster", serviceConfigRouter)
        systemRouter.actorOf(Props[CollectorRouter], "collector-router")
      //启动GetFileWorker
      case "worker" =>
        val serviceConfigWorker = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=0")
          .withFallback(ConfigFactory.parseString("akka.cluster.auto-down-unreachable-after = 5s"))
          .withFallback(ConfigFactory.parseString("akka.cluster.roles = [worker]"))
          .withFallback(config)
        val systemWorker = ActorSystem("hss-cluster", serviceConfigWorker)
        systemWorker.actorOf(Props[GetFileWorker], "getfile-worker")
    }
  } catch {
    case e: Throwable =>
      val formatter = new HelpFormatter()
      consoleLog("ERROR", s"${e.getMessage}, ${e.getStackTrace.map{_.toString}.mkString("\n")}")
      formatter.printHelp("Start HSS log collector cluster.\n", options, true)
  }
}
