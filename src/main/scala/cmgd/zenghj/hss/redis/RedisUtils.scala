package cmgd.zenghj.hss.redis

import cmgd.zenghj.hss.common.CommonUtils._

import redis.clients.jedis._

import scala.collection.JavaConversions._

/**
  * Created by cookeem on 16/7/27.
  */
object RedisUtils {
  val jedis = new JedisCluster(configRedisHosts)

  //更新hss:dirs表
  def redisDirsUpdate(dirs: Array[String]) = {
    try {
      dirs.foreach { dir =>
        jedis.sadd("hss:dir", dir)
      }
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"redisDirsUpdate error: ${e.getMessage}, ${e.getCause}")
    }
  }

  //更新文件列表,返回新增文件清单Array[filename],不包含dir
  def redisFilesInsert(dir: String, filesArr: Array[String]): Array[String] = {
    var newFileArr = Array[String]()
    try {
      if (filesArr.nonEmpty) {
        var currentLastFile = jedis.get(s"hss:dir:$dir:lastfile")
        if (currentLastFile == null) {
          currentLastFile = ""
        }
        val lastfile = filesArr.last
        if (lastfile > currentLastFile) {
          jedis.set(s"hss:dir:$dir:lastfile", lastfile)
          filesArr.foreach { filename =>
            if (filename > currentLastFile) {
              jedis.sadd(s"hss:dir:$dir:files", filename)
              newFileArr = newFileArr :+ filename
            }
          }
        }
      }
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"redisFilesInsert error: ${e.getMessage}, ${e.getCause}")
    }
    newFileArr
  }

  def redisFileAvail(dir: String, filename: String) = {
    var available = false
    try {
      val found = jedis.sismember(s"hss:dir:$dir:files", filename)
      if (found) {
        available = true
      }
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"redisFileAvail error: ${e.getMessage}, ${e.getCause}")
    }
    available
  }

  def redisFileRemove(dir: String, filename: String) = {
    var success = false
    try {
      jedis.srem(s"hss:dir:$dir:files", filename)
      success = true
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"redisFileRemove error: ${e.getMessage}, ${e.getCause}")
    }
    success
  }

  def redisFilesGet(dir: String): Array[String] = {
    var files = Array[String]()
    try {
      jedis.smembers(s"hss:dir:$dir:files").foreach { file =>
        files = files :+ file
      }
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"redisFilesGet error: ${e.getMessage}, ${e.getCause}")
    }
    files
  }

  def redisProcessingFileInsert(dir: String, filename: String) = {
    var success = false
    try {
      jedis.sadd(s"hss:processingfiles", s"$dir:$filename")
      success = true
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"redisProcessingFileInsert error: ${e.getMessage}, ${e.getCause}")
    }
    success
  }

  def redisProcessingFilesGet() = {
    var files = Array[(String, String)]()
    try {
      jedis.smembers(s"hss:processingfiles").foreach { file =>
        val fileSplit = file.split(":")
        if (fileSplit.length == 2) {
          val dir = fileSplit(0)
          val filename = fileSplit(1)
          files = files :+ (dir, filename)
        }
      }
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"redisProcessingFilesGet error: ${e.getMessage}, ${e.getCause}")
    }
    files
  }

  def redisProcessingFileRemove(dir: String, filename: String) = {
    var success = false
    try {
      jedis.srem(s"hss:processingfiles", s"$dir:$filename")
      success = true
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"redisProcessingFileRemove error: ${e.getMessage}, ${e.getCause}")
    }
    success
  }

  def redisProcessingFileDeleteAll() = {
    var success = false
    try {
      jedis.del(s"hss:processingfiles")
      success = true
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"redisProcessingFileDeleteAll error: ${e.getMessage}, ${e.getCause}")
    }
    success
  }

}
