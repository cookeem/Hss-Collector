package cmgd.zenghj.hss

import java.net.InetAddress

import cmgd.zenghj.hss.actor._
import cmgd.zenghj.hss.common.CommonUtils._
import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import org.apache.commons.cli.{DefaultParser, HelpFormatter, Options, Option => CliOption}

import scala.concurrent.duration._

/**
  * Created by cookeem on 16/7/28.
  */
object HssCollector extends App {
  val options = new Options()
  //单参数选项
  options.addOption(CliOption.builder("s").longOpt("service")
    .desc("Service name: master or worker")
    .hasArg(true)
    .required(true)
    .argName("SERVICE")
    .build())
  //可选参数选项
  options.addOption(CliOption.builder("n").longOpt("nat")
    .desc("Is NAT: if run in docker or k8s must use NAT")
    .hasArg(false)
    .required(false)
    .build())
  //单参数选项
  options.addOption(CliOption.builder("p").longOpt("port")
    .desc("Worker port: int number")
    .hasArg(true)
    .required(false)
    .argName("PORT")
    .build())
  try {
    val parser = new DefaultParser()
    val cmd = parser.parse(options, args)
    val nat = cmd.hasOption("n")
    val service = cmd.getOptionValue("s")
    if (service != "master" && service != "worker") {
      throw new Throwable()
    }
    //当service == worker的时候需要port参数
    var port = 0
    if (cmd.hasOption("p")) {
      port = cmd.getOptionValue("p").toInt
    } else {
      port = 0
    }
    val hostName = InetAddress.getLocalHost.getHostName
    //启动处理逻辑
    service match {
      //启动CollectorMaster
      case "master" =>
        //启动单个master，端口2551
        var serviceConfigMaster = ConfigFactory.parseString(s"akka.cluster.roles = [master]")
          .withFallback(ConfigFactory.parseString(s"akka.cluster.auto-down-unreachable-after = 5s"))
        if (!nat) {
          serviceConfigMaster = serviceConfigMaster
            .withFallback(ConfigFactory.parseString(s"""akka.remote.netty.tcp.hostname="$hostName""""))
            .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.port=2551"))
            .withFallback(config)
        } else {
          serviceConfigMaster = serviceConfigMaster
            .withFallback(ConfigFactory.parseString(s"""akka.remote.netty.tcp.hostname="$hostName""""))
            .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.port=0"))
            .withFallback(ConfigFactory.parseString(s"""akka.remote.netty.tcp.bind-hostname="$hostName""""))
            .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.bind-port=2551"))
            .withFallback(config)
        }
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
        var serviceConfigRouter = ConfigFactory.parseString(s"akka.cluster.roles = [router]")
          .withFallback(ConfigFactory.parseString(s"akka.cluster.auto-down-unreachable-after = 5s"))
        if (!nat) {
          serviceConfigRouter = serviceConfigRouter
            .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.hostname=$hostName"))
            .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.port=2552"))
            .withFallback(config)
        } else {
          serviceConfigRouter = serviceConfigRouter
            .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.hostname=$hostName"))
            .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.port=0"))
            .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.bind-hostname=$hostName"))
            .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.bind-port=2552"))
            .withFallback(config)
        }
        val systemRouter = ActorSystem("hss-cluster", serviceConfigRouter)
        systemRouter.actorOf(Props[CollectorRouter], "collector-router")
      //启动GetFileWorker
      case "worker" =>
        var serviceConfigWorker = ConfigFactory.parseString(s"akka.cluster.roles = [worker]")
          .withFallback(ConfigFactory.parseString("akka.cluster.auto-down-unreachable-after = 5s"))
        if (!nat) {
          serviceConfigWorker = serviceConfigWorker
            .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.hostname=$hostName"))
            .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port"))
            .withFallback(config)
        } else {
          serviceConfigWorker = serviceConfigWorker
            .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.hostname=$hostName"))
            .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.port=0"))
            .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.bind-hostname=$hostName"))
            .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.bind-port=$port"))
            .withFallback(config)
        }
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
