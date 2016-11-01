package cmgd.zenghj.hss.common

import java.io.File

import com.typesafe.config.ConfigFactory
import org.joda.time.DateTime

import scala.collection.JavaConversions._

/**
  * Created by cookeem on 16/7/28.
  */
object CommonUtils {
  val config = ConfigFactory.parseFile(new File("conf/application.conf"))

  val configMaster = config.getConfig("cluster-router.master")
  val configMasterScheduleInterval = configMaster.getInt("schedule-interval")
  val configMasterStatInterval = configMaster.getInt("stat-interval")

  val configListFileRouter = config.getConfig("cluster-router.listfile-router")
  val configListFileRouterPoolSize = configListFileRouter.getInt("pool-size")
  val configListFileRouterLowBound = configListFileRouter.getInt("lower-bound")
  val configListFileRouterUpperBound = configListFileRouter.getInt("upper-bound")

  val configGetFileWorkerConfig = config.getConfig("cluster-router.getfile-worker")
  val configGetFileWorkerStreamCount = configGetFileWorkerConfig.getInt("stream-count")

  val configFtp = config.getConfig("ftp")
  val configFtpHost = configFtp.getString("ftp-host")
  val configFtpPort = configFtp.getInt("ftp-port")
  val configFtpUser = configFtp.getString("ftp-user")
  val configFtpPass = configFtp.getString("ftp-pass")
  val configFtpRoot = configFtp.getString("ftp-root")
  val configFtpLocalRoot = configFtp.getString("ftp-localroot")

  val configRedis = config.getConfig("redis")
  val configRedisHost = configRedis.getString("redis-host")
  val configRedisPort = configRedis.getInt("redis-port")
  var configRedisPass = configRedis.getString("redis-pass")

  val configKafka = config.getConfig("kafka")
  val configKafkaZkUri = configKafka.getString("zookeeper-uri")
  val configKafkaBrokers = configKafka.getString("brokers-list")
  val configKafkaFilesTopic = configKafka.getString("kafka-files-topic")
  val configKafkaRecordsTopic = configKafka.getString("kafka-records-topic")
  val configKafkaConsumeGroup = configKafka.getString("consume-group")
  val configKafkaNumPartitions = configKafka.getInt("kafka-num-partitions")
  val configKafkaReplication = configKafka.getInt("kafka-replication")

  val configEs = config.getConfig("elasticsearch")
  val configEsNodes = configEs.getString("nodes")
  val configEsClusterName = configEs.getString("cluster-name")
  val configEsHosts: Array[(String, Int)] = configEs.getConfigList("hosts").map { conf =>
    (conf.getString("host"), conf.getInt("port"))
  }.toArray
  val configEsIndexName = configEs.getString("index-name")
  val configEsTypeName = configEs.getString("type-name")
  val configEsNumberOfShards = configEs.getInt("number-of-shards")
  val configEsNumberOfReplicas = configEs.getInt("number-of-replicas")

  def consoleLog(logType: String, msg: String) = {
    val timeStr = new DateTime().toString("yyyy-MM-dd HH:mm:ss")
    println(s"[$logType] $timeStr: $msg")
  }
}
