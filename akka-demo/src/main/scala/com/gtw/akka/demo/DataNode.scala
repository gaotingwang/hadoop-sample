package com.gtw.akka.demo

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.gtw.akka.demo.model.{RegisterWorker, RegisteredWorker, HeartBeat, SendHeartBeat}
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._

import java.util.UUID

class DataNode extends Actor{

  var masterRef: ActorSelection = _
  val WORKER_ID = UUID.randomUUID().toString

  // 第三步：声明周期方法，启动时向NameNode发送注册消息
  override def preStart(): Unit = {
    masterRef = context.actorSelection("akka.tcp://MASTER_ACTOR_SYSTEM@localhost:9527/user/MASTER_ACTOR")
    masterRef ! RegisterWorker(WORKER_ID, 4096, 8)
  }

  // 接收消息
  override def receive: Receive = {
    // 接收到注册成功回执消息
    case RegisteredWorker => {
      println("接收到在Master注册成功消息...")

      // 第六步：定期发送心跳给NN
      import context.dispatcher
      // 给自己发送了消息
      context.system.scheduler.schedule(0 milliseconds, 5000 milliseconds, self, SendHeartBeat)
    }
    case SendHeartBeat => {
      masterRef ! HeartBeat(WORKER_ID)
    }
    case "hi" => println("hi ...")
    case _ => println("nothing ...")
  }
}

object DataNode {

  def main(args: Array[String]): Unit = {
    // 第二步：启动DN
    val host = "localhost"
    val port = 9528
    val conf = ConfigFactory.parseString(
      s"""
         |akka.loglevel=ERROR
         |akka.actor.provider="akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname="$host"
         |akka.remote.netty.tcp.port="$port"
         |""".stripMargin)
    val workerActorSystem = ActorSystem("WORKER_ACTOR_SYSTEM", conf)

    val workerActorRef = workerActorSystem.actorOf(Props[DataNode], "WORKER_ACTOR")

    // 消息发送
//    workerActorRef ! "hi"

  }

}
