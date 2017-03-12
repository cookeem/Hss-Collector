package cmgd.zenghj.hss.actor

import akka.actor.{Actor, ActorLogging}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import cmgd.zenghj.hss.common.CommonUtils.consoleLog

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

/**
  * Created by cookeem on 16/7/25.
  */
trait TraitClusterActor extends Actor with ActorLogging {
  val cluster = Cluster(context.system)

  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
    consoleLog("INFO", s"actor start! ${self.path}")
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
    context.system.terminate()
    consoleLog("INFO", s"actor stop! ${self.path}")
  }

  def eventReceive: Receive = {
    case LeaderChanged(addr) =>
      log.info(s"Leader changed: $addr")
    case MemberJoined(member) =>
      log.info(s"Member joined: ${member.address}")
    case UnreachableMember(member) =>
      log.error(s"Member Unreachable: ${member.address}")
      cluster.leave(member.address)
    case MemberExited(member) =>
      log.error(s"Member is Exited: ${member.address}")
    case MemberLeft(member) =>
      log.error(s"Member is Left: ${member.address}")
  }

  //定义集群member removed关闭事件
  cluster.registerOnMemberRemoved{
    //更新seed nodes
    cluster.system.registerOnTermination(System.exit(0))
    cluster.system.terminate()
    new Thread {
      override def run(): Unit = {
        if (Try(Await.ready(cluster.system.whenTerminated, 10.seconds)).isFailure)
          System.exit(-1)
      }
    }.start()
  }
}
