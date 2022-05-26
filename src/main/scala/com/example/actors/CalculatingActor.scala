package com.example.actors

import akka.actor.{Actor, Props}

object CalculatingActor {
  case class PerformCalculation(number: Int)

  def props(): Props = Props(new CalculatingActor)
}

class CalculatingActor extends Actor {
  import CalculatingActor._

  override def receive: Receive = {
    case PerformCalculation(number) =>
      println(s"[${self.path}] Calculating $number * 2")
      sender() ! number * 2
  }
}
