package cmgd.zenghj.hss.actor

/**
  * Created by cookeem on 16/7/27.
  */

//列表获取所有目录指令
case object DirectiveListDir
//让ListFileWorker列出目录下所有文件的指令
case class DirectiveListFile(dir: String)
//发送指令,从GetFileWorker获取统计信息的指令
case object DirectiveStat
//发送指令,从GetFileWorker获取统计信息的指令
case class DirectiveStatResult(fileCount: Int, fileFailCount: Int, recordCount: Int)
//发送指令,GetFileWorker获取记录处理统计信息的指令
case class DirectiveRecordCount(dir: String, filename: String, filenameTxt: String, recordCount: Int)


