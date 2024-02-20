package com.gtw.akka.demo.model

case class RegisterWorker(id: String, memory: Int, cores: Int)

case object RegisteredWorker

case class HeartBeat(workerId: String)

case object SendHeartBeat

case object CheckWorkerTimeout
