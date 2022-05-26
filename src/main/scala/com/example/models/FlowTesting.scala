package com.example.models

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.stream.{ActorAttributes, ActorMaterializer, Attributes, Supervision}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import com.example.actors.{CalculatingActor, StreamMonitor}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

object FlowTesting extends App {

  implicit val actorSystem: ActorSystem = ActorSystem("kafka-consumer")
  implicit val materializer: ActorMaterializer.type = ActorMaterializer
  implicit val adapter: LoggingAdapter = Logging(actorSystem, "logger")
  implicit val timeout: Timeout = Timeout(10.seconds)
  val Parallelism: Int = 5

  val streamMonitorActor = actorSystem.actorOf(StreamMonitor.props(), "stream-monitor-actor")
  val calculatingActor = actorSystem.actorOf(CalculatingActor.props(), "calculating-actor")

  val logAttributes = Attributes.logLevels(
    onElement = Attributes.LogLevels.Info,
    onFinish = Attributes.LogLevels.Info,
    onFailure = Attributes.LogLevels.Error
  )

  val decider: Supervision.Decider = {
    case _: IllegalStateException => Supervision.Resume
    case _ => Supervision.Stop
  }

  val source: Source[Int, NotUsed] = Source(1 to 20)
  val successFlow: Flow[Int, Int, NotUsed] = {
    Flow[Int].mapAsync(Parallelism) { element =>
      (calculatingActor ? CalculatingActor.PerformCalculation(element))
        .mapTo[Int]
    }
  }

  val failingFlow: Flow[Int, Int, NotUsed] = {
    Flow[Int].map { element =>
      if(element == 6) throw new IllegalStateException("Something Failed")
      else element * 2
    }
  }.withAttributes(ActorAttributes.supervisionStrategy(decider))

  val sink: Sink[Any, Future[Done]] = Sink.ignore

  val monitoredStream =
    source
    // Watch termination allows us to monitor final status of every stage on our Stream
    .watchTermination()((prev, future) => {
      future pipeTo streamMonitorActor
    })
    // Allows to store messages on buffer from a faster upstream, OverflowStrategy will
    // decide what to do if buffer fills
    //.buffer(20, OverflowStrategy.backpressure)
    .log(s"Source")
    .addAttributes(logAttributes)
    // Adding async allows elements passed downstream to be executed asynchronously
    .async
    .via(successFlow)
    .log(s"Success Flow")
    .addAttributes(logAttributes)
    // mapAsync allows to pass incoming elements to a function that returns a Future
    .mapAsync(Parallelism)(asyncCalculationFunc)
    .log(s"Async Calc Flow")
    .addAttributes(logAttributes)
    .runWith(sink)

  def asyncCalculationFunc(element: Int): Future[Int] = {
    Future.successful(element * 3)
  }
}
