package cmgd.zenghj.hss.common

import java.io.File

import com.typesafe.config.ConfigFactory
import org.joda.time.DateTime
import redis.clients.jedis.HostAndPort

import scala.collection.JavaConversions._

/**
  * Created by cookeem on 16/7/28.
  */
object CommonUtils {
  val config = ConfigFactory.parseFile(new File("conf/application.conf"))

  val configCollectorMaster = config.getConfig("collector-master")
  val configMasterScheduleInterval = configCollectorMaster.getInt("schedule-interval")
  val configMasterStatInterval = configCollectorMaster.getInt("stat-interval")

  val configFtp = config.getConfig("ftp")
  val configFtpHost = configFtp.getString("ftp-host")
  val configFtpPort = configFtp.getInt("ftp-port")
  val configFtpUser = configFtp.getString("ftp-user")
  val configFtpPass = configFtp.getString("ftp-pass")
  val configFtpRemoteRoot = configFtp.getString("ftp-remote-root")
  var configFtpLocalRoot = configFtp.getString("ftp-local-root")

  val configHttp = config.getConfig("http")
  val configHttpPort = configHttp.getInt("port")

  val configRedis = config.getConfig("redis")
  val configRedisHosts = configRedis.getConfigList("hosts").map { cfg =>
    (cfg.getString("redis-host"), cfg.getInt("redis-port"), cfg.getString("redis-password"))
  }

  val configKafka = config.getConfig("kafka")
  val configKafkaZkUri = configKafka.getString("zookeeper-uri")
  val configKafkaBrokers = configKafka.getString("brokers-list")
  val configKafkaFilesTopic = configKafka.getString("kafka-files-topic")
  val configKafkaConsumeGroup = configKafka.getString("consume-group")
  val configKafkaNumPartitions = configKafka.getInt("kafka-num-partitions")
  val configKafkaReplication = configKafka.getInt("kafka-replication")

  val configEs = config.getConfig("elasticsearch")
  val configEsClusterName = configEs.getString("cluster-name")
  val configEsUserName = configEs.getString("es-username")
  val configEsPassword = configEs.getString("es-password")
  val configEsHosts: Array[(String, Int)] = configEs.getConfigList("hosts").map { conf =>
    (conf.getString("host"), conf.getInt("port"))
  }.toArray
  val configEsIndexName = configEs.getString("index-name")
  val configEsTypeNameEric = configEs.getString("type-name-eric")
  val configEsTypeNameHuawei = configEs.getString("type-name-huawei")
  val configEsNumberOfShards = configEs.getInt("number-of-shards")
  val configEsNumberOfReplicas = configEs.getInt("number-of-replicas")

  def consoleLog(logType: String, msg: String) = {
    val timeStr = new DateTime().toString("yyyy-MM-dd HH:mm:ss")
    println(s"[$logType] $timeStr: $msg")
  }

  //从参数Map中获取Int
  def paramsGetInt(params: Map[String, String], key: String, default: Int): Int = {
    var ret = default
    if (params.contains(key)) {
      try {
        ret = params(key).toInt
      } catch {
        case e: Throwable =>
      }
    }
    ret
  }

  //从参数Map中获取String
  def paramsGetString(params: Map[String, String], key: String, default: String): String = {
    var ret = default
    if (params.contains(key)) {
      ret = params(key)
    }
    ret
  }
}
