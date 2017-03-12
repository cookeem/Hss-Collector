package cmgd.zenghj.hss.ftp

import cmgd.zenghj.hss.common.CommonUtils._

import java.io.{FileOutputStream, File, FileInputStream}

import org.apache.commons.net.ftp.{FTPFile, FTP, FTPClient}

/**
  * Created by cookeem on 16/7/25.
  */
case class FtpUtils(ftpHost: String, ftpPort: Int, ftpUser: String, ftpPass: String) {
  //连接ftp
  def connect(): FTPClient = {
    var ftpClient = new FTPClient()
    try {
      //连接
      ftpClient.connect(ftpHost, ftpPort)
      val isLogin = ftpClient.login(ftpUser, ftpPass)
      if (isLogin) {
        //设置
        ftpClient.enterLocalPassiveMode()
        ftpClient.setFileType(FTP.BINARY_FILE_TYPE)
        ftpClient.setControlKeepAliveTimeout(20)
      } else {
        //登录失败则断开连接
        ftpClient.disconnect()
        ftpClient = null
        consoleLog("ERROR", s"ftp connect login error: ftpHost = $ftpHost, ftpPort = $ftpPort, ftpUser = $ftpUser, ftpPass = $ftpPass. ${ftpClient.getReplyString}")
      }
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"ftp connect error: ftpHost = $ftpHost, ftpPort = $ftpPort, ftpUser = $ftpUser, ftpPass = $ftpPass. ${ftpClient.getReplyString}")
        ftpClient.disconnect()
        ftpClient = null
        consoleLog("ERROR", s"ftp connect error: ftpHost = $ftpHost, ftpPort = $ftpPort, ftpUser = $ftpUser, ftpPass = $ftpPass. ${e.getClass}, ${e.getMessage}, ${e.getCause}")
    }
    ftpClient
  }

  //ftp上传文件
  def put(localPath: String, localFileName: String, remotePath: String, remoteFileName: String): Boolean = {
    var success = false
    val ftpClient = connect()
    try {
      if (ftpClient != null) {
        //上传
        val is = new FileInputStream(new File(s"$localPath/$localFileName"))
        ftpClient.storeFile(s"$remotePath$remoteFileName", is)
        is.close()
        ftpClient.disconnect()
        success = true
      } else {
        consoleLog("ERROR", s"ftp put error: ftpHost = $ftpHost, ftpPort = $ftpPort, ftpUser = $ftpUser, ftpPass = $ftpPass")
      }
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"ftp put error: localPath = $localPath, localFileName = $localFileName, remotePath = $remotePath, remoteFileName = $remoteFileName. ${e.getClass}, ${e.getMessage}, ${e.getCause}")
    }
    success
  }

  //ftp下载文件
  def get(localPath: String, localFileName: String, remotePath: String, remoteFileName: String): Boolean = {
    var success = false
    val ftpClient = connect()
    var errmsg = ""
    val startTime = System.currentTimeMillis()
    try {
      if (ftpClient != null) {
        //下载
        val fos = new FileOutputStream(s"$localPath/$localFileName")
        ftpClient.setBufferSize(1024)
        success = ftpClient.retrieveFile(s"$remotePath/$remoteFileName", fos)
        if (!success) {
          errmsg = s"ftp get error: localPath = $localPath, localFileName = $localFileName, remotePath = $remotePath, remoteFileName = $remoteFileName. file not exists"
        }
      } else {
        errmsg = s"ftp get error: localPath = $localPath, localFileName = $localFileName, remotePath = $remotePath, remoteFileName = $remoteFileName. not connected"
      }
      ftpClient.disconnect()
    } catch {
      case e: Throwable =>
        errmsg = s"ftp get error: localPath = $localPath, localFileName = $localFileName, remotePath = $remotePath, remoteFileName = $remoteFileName. ${e.getClass}, ${e.getMessage}, ${e.getCause}"
    }
    val duration = Math.round(System.currentTimeMillis() - startTime)
    if (errmsg != "") {
      consoleLog("ERROR", s"$errmsg # took $duration ms")
    } else {
      consoleLog("INFO", s"ftp get $remotePath/$remoteFileName success # took $duration ms")
    }
    success
  }

  //ftp列表, mode(0:全部,1:目录,2:文件)
  def ls(remotePath: String, mode: Int = 0): Seq[FTPFile] = {
    var ret = Seq[FTPFile]()
    val ftpClient = connect()
    var errmsg = ""
    val startTime = System.currentTimeMillis()
    try {
      if (ftpClient != null) {
        ret = mode match {
          case 0 => ftpClient.listFiles(remotePath)
          case 1 => ftpClient.listFiles(remotePath).filter(f => f.isDirectory)
          case 2 => ftpClient.listFiles(remotePath).filter(f => f.isFile)
          case _ => ftpClient.listFiles(remotePath)
        }
        ftpClient.disconnect()
      } else {
        errmsg = s"ftp list error: ftpHost = $ftpHost, ftpPort = $ftpPort, ftpUser = $ftpUser, ftpPass = $ftpPass"
      }
    } catch {
      case e: Throwable =>
        errmsg = s"ftp list error: remotePath = $remotePath, mode = $mode. ${e.getClass}, ${e.getMessage}, ${e.getCause}"
    }
    val duration = Math.round(System.currentTimeMillis() - startTime)
    if (errmsg != "") {
      consoleLog("ERROR", s"$errmsg # took $duration ms")
    } else {
//      consoleLog("SUCCESS", s"ftp ls $remotePath success # took $duration ms")
    }
    ret
  }

}
