package cmgd.zenghj.hss.redis

import cmgd.zenghj.hss.common.CommonUtils._

import redis.clients.jedis._

import scala.collection.JavaConversions._

/**
  * Created by cookeem on 16/7/27.
  */
object RedisUtils {
  var redisPool = redisConnect()

  def redisConnect(): ShardedJedisPool = {
    var pool: ShardedJedisPool = null
    try {
      val shards = configRedisHosts.map { case (redisHost, redisPort, redisPassword) =>
        val jsi = new JedisShardInfo(redisHost, redisPort)
        if (redisPassword != "") {
          jsi.setPassword(redisPassword)
        }
        jsi
      }
      pool = new ShardedJedisPool(new JedisPoolConfig(), shards)
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"redis not connected error: ${e.getClass}, ${e.getMessage}, ${e.getCause}")
    }
    pool
  }

  //监控redis是否连接正常
  def redisCheck(): String = {
    var errmsg = ""
    var jedis: ShardedJedis = null
    try {
      jedis = redisPool.getResource
      (1 to 20).foreach { i =>
        jedis.set(s"test-key-$i", s"test-value-$i")
        jedis.del(s"test-key-$i")
      }
    } catch {
      case e: Throwable =>
        errmsg = s"redis not connected error: ${e.getClass}, ${e.getMessage}, ${e.getCause}"
        consoleLog("ERROR", errmsg)
    } finally {
      jedis.close()
    }
    errmsg
  }

  //更新文件列表,返回新增文件清单Array[filename],不包含dir
  //neType（网元类型： 0:eric，1:huawei）
  def redisFilesInsert(dir: String, filesArr: Seq[String]): Seq[String] = {
    var neName = "eric"
    if (dir.indexOf("_HW/") > 0) {
      neName = "huawei"
    }
    var newFileArr = Seq[String]()
    var jedis: ShardedJedis = null
    try {
      jedis = redisPool.getResource
      if (filesArr.nonEmpty) {
        var currentLastFile = jedis.get(s"hss-$neName:dir:$dir:lastfile")
        if (currentLastFile == null) {
          currentLastFile = ""
        }
        val lastfile = filesArr.last
        if (lastfile > currentLastFile) {
          jedis.set(s"hss-$neName:dir:$dir:lastfile", lastfile)
          filesArr.foreach { filename =>
            if (filename > currentLastFile) {
              jedis.sadd(s"hss-$neName:dir:$dir:files", filename)
              newFileArr = newFileArr :+ filename
            }
          }
        }
      }
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"redisFilesInsert error: ${e.getClass}, ${e.getMessage}, ${e.getCause}")
    } finally {
      jedis.close()
    }
    newFileArr
  }

  //neType（网元类型： 0:eric，1:huawei）
  def redisFileRemove(dir: String, filename: String) = {
    var neName = "eric"
    if (dir.indexOf("_HW/") > 0) {
      neName = "huawei"
    }
    var success = false
    var jedis: ShardedJedis = null
    try {
      jedis = redisPool.getResource
      jedis.srem(s"hss-$neName:dir:$dir:files", filename)
      success = true
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"redisFileRemove error: ${e.getClass}, ${e.getMessage}, ${e.getCause}")
    } finally {
      jedis.close()
    }
    success
  }

  //neType（网元类型： 0:eric，1:huawei）
  def redisProcessingFileInsert(dir: String, filename: String) = {
    var neName = "eric"
    if (dir.indexOf("_HW/") > 0) {
      neName = "huawei"
    }
    var success = false
    var jedis: ShardedJedis = null
    try {
      jedis = redisPool.getResource
      jedis.sadd(s"hss-$neName:processingfiles", s"$dir:$filename")
      success = true
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"redisProcessingFileInsert error: ${e.getClass}, ${e.getMessage}, ${e.getCause}")
    } finally {
      jedis.close()
    }
    success
  }

  //neType（网元类型： 0:eric，1:huawei）
  def redisProcessingFilesGet(neType: Int = 0) = {
    var neName = "eric"
    if (neType == 1) {
      neName = "huawei"
    }
    var files = Seq[(String, String)]()
    var jedis: ShardedJedis = null
    try {
      jedis = redisPool.getResource
      jedis.smembers(s"hss-$neName:processingfiles").foreach { file =>
        val fileSplit = file.split(":")
        if (fileSplit.length == 2) {
          val dir = fileSplit(0)
          val filename = fileSplit(1)
          files = files :+ (dir, filename)
        }
      }
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"redisProcessingFilesGet error: ${e.getClass}, ${e.getMessage}, ${e.getCause}")
    } finally {
      jedis.close()
    }
    files
  }

  //neType（网元类型： 0:eric，1:huawei）
  def redisProcessingFileRemove(dir: String, filename: String) = {
    var neName = "eric"
    if (dir.indexOf("_HW/") > 0) {
      neName = "huawei"
    }
    var success = false
    var jedis: ShardedJedis = null
    try {
      jedis = redisPool.getResource
      jedis.srem(s"hss-$neName:processingfiles", s"$dir:$filename")
      success = true
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"redisProcessingFileRemove error: ${e.getClass}, ${e.getMessage}, ${e.getCause}")
    } finally {
      jedis.close()
    }
    success
  }

  //neType（网元类型： 0:eric，1:huawei）
  def redisProcessingFileDeleteAll(neType: Int = 0) = {
    var neName = "eric"
    if (neType == 1) {
      neName = "huawei"
    }
    var success = false
    var jedis: ShardedJedis = null
    try {
      jedis = redisPool.getResource
      jedis.del(s"hss-$neName:processingfiles")
      success = true
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"redisProcessingFileDeleteAll error: ${e.getClass}, ${e.getMessage}, ${e.getCause}")
    } finally {
      jedis.close()
    }
    success
  }

}
