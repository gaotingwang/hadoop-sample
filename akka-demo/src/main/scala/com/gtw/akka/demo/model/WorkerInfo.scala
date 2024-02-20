package com.gtw.akka.demo.model

class WorkerInfo(val id: String, val memory: Int, val cores: Int) {

  var lastUpdateTime: Long = _
  override def toString: String = s"WorkerInfo($id, $memory, $cores)"
}
