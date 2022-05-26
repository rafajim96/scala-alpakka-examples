package com.example

import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.{ActorMaterializer, ThrottleMode}
import akka.stream.scaladsl.Source
import com.example.common.KafkaTopics
import com.example.models.Movie
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}
import spray.json.enrichAny
import com.example.common.JsonFormats._
import scala.concurrent.duration.{FiniteDuration, SECONDS}

object KafkaPublisher extends App {

  implicit val actorSystem: ActorSystem = ActorSystem("kafka-publisher")
  implicit val materializer: ActorMaterializer.type = ActorMaterializer

  val producerSettings = ProducerSettings(actorSystem, new IntegerSerializer, new StringSerializer)

  Source(1 to 10)
    .throttle(1, FiniteDuration(1, SECONDS), 1, ThrottleMode.Shaping)
    .map(num => {
      // construct msg here
      val movieToPublish = Movie(num, s"Movie $num", isActive = true)
      val message = s"Akka Scala Producer Message New # ${movieToPublish.toJson.compactPrint}"
      println(s"Message sent to topic - ${KafkaTopics.TestTopic} - $message")

      new ProducerRecord(KafkaTopics.TestTopic, Int.box(movieToPublish.id), movieToPublish.toJson.compactPrint)
    })
    .runWith(Producer.plainSink(producerSettings))
    .onComplete(_ => {
      println("All messages sent!")
      actorSystem.terminate
    })(scala.concurrent.ExecutionContext.global)

}
