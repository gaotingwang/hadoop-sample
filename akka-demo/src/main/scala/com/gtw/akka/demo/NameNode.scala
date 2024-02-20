package com.gtw.akka.demo

import akka.actor.{Actor, ActorSystem, Props}
import com.gtw.akka.demo.model.{HeartBeat, RegisterWorker, RegisteredWorker, WorkerInfo, CheckWorkerTimeout}
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._

import scala.collection.mutable

class NameNode extends Actor{

  val workerids = new mutable.HashMap[String, WorkerInfo]()

  // 第七步：周期方法，定期检查DN是否存活
  override def preStart(): Unit = {
    import context.dispatcher
    // 给自己发送了消息
    context.system.scheduler.schedule(0 milliseconds, 10 seconds, self, CheckWorkerTimeout)
  }

  // 接收消息
  override def receive: Receive = {
    case RegisterWorker(id, memory, cores) => {
      // 第四步：接收注册消息并记录
      println(s"Master 接收到 Worker[$id] 发送过来的消息")
      val info = new WorkerInfo(id, memory, cores)
      workerids(id) = info
      // 第五步：接收成功后，向DN发送回执
      sender() ! RegisteredWorker
    }
    case HeartBeat(workerId) => {
      if(workerids.contains(workerId)) {
        println(s"接收 $workerId 的心跳")
        val workerInfo = workerids(workerId)
        workerInfo.lastUpdateTime = System.currentTimeMillis()
      }
    }
    case CheckWorkerTimeout => {
      val currentTime = System.currentTimeMillis()
      val deadWorkers = workerids.values.filter(x => currentTime - x.lastUpdateTime > 10 * 1000L)
      deadWorkers.foreach(x => {
        workerids -= x.id
        println(s"${x.id} 失去心跳，进行移除...")
      })
      println(s"当前存活worker数量为：${workerids.size}")
    }
    case "register" => println("receive worker register...")
    case "hello" => println("receive hello")
    case "world" => println("receive world")
    case _ => println("receive others")
  }
}

object NameNode {

  def main(args: Array[String]): Unit = {
    // 第一步:启动NN
    val host = "localhost"
    val port = 9527
    val conf = ConfigFactory.parseString(
      s"""
        |akka.loglevel=INFO
        |akka.actor.provider="akka.remote.RemoteActorRefProvider"
        |akka.remote.netty.tcp.hostname="$host"
        |akka.remote.netty.tcp.port="$port"
        |""".stripMargin)
    val masterActorSystem = ActorSystem("MASTER_ACTOR_SYSTEM", conf)

    val masterActorRef = masterActorSystem.actorOf(Props[NameNode], "MASTER_ACTOR")

    // 消息发送
//    masterActorRef ! "hello"
//    masterActorRef ! "world"

  }
}
