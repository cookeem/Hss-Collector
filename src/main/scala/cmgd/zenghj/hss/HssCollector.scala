package cmgd.zenghj.hss

import java.net.InetAddress

import cmgd.zenghj.hss.actor._
import cmgd.zenghj.hss.common.CommonUtils._
import akka.actor.{ActorSystem, Props}
import cmgd.zenghj.hss.es.EsUtils._
import cmgd.zenghj.hss.redis.RedisUtils.{redisConnect, redisPool, redisCheck}
import cmgd.zenghj.hss.kafka.KafkaUtils.kafkaCreateTopic
import com.typesafe.config.ConfigFactory
import org.apache.commons.cli.{DefaultParser, HelpFormatter, Options, Option => CliOption}

import scala.concurrent.duration._

/**
  * Created by cookeem on 16/7/28.
  */
object HssCollector extends App {
  //等待redis连接成功
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


  val options = new Options()
  //单参数选项
  options.addOption(CliOption.builder("s").longOpt("service")
    .desc("Service name: master or listfile or getfile")
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
  //可选参数选项
  options.addOption(CliOption.builder("d").longOpt("download-dir")
    .desc("download dir: ftp download local directory")
    .hasArg(true)
    .required(false)
    .argName("DIR")
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
    if (service != "master" && service != "listfile" && service != "getfile") {
      throw new Throwable()
    }
    //当service == worker的时候需要port参数
    var port = 0
    if (cmd.hasOption("p")) {
      port = cmd.getOptionValue("p").toInt
    } else {
      port = 0
    }
    //检查download-dir参数
    if (cmd.hasOption("d")) {
      configFtpLocalRoot = cmd.getOptionValue("d")
    }
    val hostName = InetAddress.getLocalHost.getHostName
    val hostAddress = InetAddress.getLocalHost.getHostAddress
    //启动处理逻辑
    service match {
      //启动CollectorMaster
      case "master" =>
        //启动单个master，端口2551
        var serviceConfigMaster = ConfigFactory.parseString(s"akka.cluster.roles = [master]")
        if (!nat) {
          serviceConfigMaster = serviceConfigMaster
            .withFallback(ConfigFactory.parseString(s"""akka.remote.netty.tcp.hostname="$hostName""""))
            .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.port=2551"))
            .withFallback(config)
        } else {
          serviceConfigMaster = serviceConfigMaster
            .withFallback(ConfigFactory.parseString(s"""akka.remote.netty.tcp.hostname="$hostName""""))
            .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.port=0"))
            .withFallback(ConfigFactory.parseString(s"""akka.remote.netty.tcp.bind-hostname="$hostAddress""""))
            .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.bind-port=2551"))
            .withFallback(config)
        }
        val systemMaster = ActorSystem("hss-cluster", serviceConfigMaster)
        val collectorMasterRef = systemMaster.actorOf(Props[CollectorMaster], "collector-master")
        import systemMaster.dispatcher
        //启动定期任务, 通知ListFileWorker进行文件夹列表任务
        systemMaster.scheduler.schedule(configMasterScheduleInterval.seconds, configMasterScheduleInterval.seconds) {
          collectorMasterRef ! DirectiveListDir
        }
        //启动定期任务, 通知GetFileWorker收集执行结果信息
        systemMaster.scheduler.schedule(configMasterStatInterval.seconds, configMasterStatInterval.seconds) {
          collectorMasterRef ! DirectiveStat
        }
      //启动ListFileWorker
      case "listfile" =>
        var serviceConfigListFile = ConfigFactory.parseString(s"akka.cluster.roles = [listfile-worker]")
        if (!nat) {
          serviceConfigListFile = serviceConfigListFile
            .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.hostname=$hostName"))
            .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port"))
            .withFallback(config)
        } else {
          serviceConfigListFile = serviceConfigListFile
            .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.hostname=$hostName"))
            .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.port=0"))
            .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.bind-hostname=$hostAddress"))
            .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.bind-port=$port"))
            .withFallback(config)
        }
        val systemListFile = ActorSystem("hss-cluster", serviceConfigListFile)
        systemListFile.actorOf(Props[ListFileWorker], "listfile-worker")
      //启动GetFileWorker
      case "getfile" =>
        var serviceConfigGetFile = ConfigFactory.parseString(s"akka.cluster.roles = [getfile-worker]")
        if (!nat) {
          serviceConfigGetFile = serviceConfigGetFile
            .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.hostname=$hostName"))
            .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port"))
            .withFallback(config)
        } else {
          serviceConfigGetFile = serviceConfigGetFile
            .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.hostname=$hostName"))
            .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.port=0"))
            .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.bind-hostname=$hostAddress"))
            .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.bind-port=$port"))
            .withFallback(config)
        }
        val systemGetFile = ActorSystem("hss-cluster", serviceConfigGetFile)
        systemGetFile.actorOf(Props[GetFileWorker], "getfile-worker")
    }
  } catch {
    case e: Throwable =>
      val formatter = new HelpFormatter()
      consoleLog("ERROR", s"${e.getClass}, ${e.getMessage}, ${e.getCause}")
      formatter.printHelp("Start HSS log collector cluster.\n", options, true)
  }
}
