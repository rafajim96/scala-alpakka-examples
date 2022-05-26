package com.example.actors

import akka.Done
import akka.actor.Status.Failure
import akka.actor.{Actor, Props}


object StreamMonitor {
  def props(): Props = Props(new StreamMonitor)
}

class StreamMonitor extends Actor {

  override def receive: Receive = {
    case Done => println(s"[${self.path}] Successful stage")
    case Failure(ex) => println(s"[${self.path}] Stream failed: ${ex.getMessage}")
  }
}
