package com.example

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.example.common.JsonFormats.movieFormat
import com.example.common.KafkaTopics
import com.example.models.Movie
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import spray.json._

import scala.concurrent.Future

object KafkaConsumer extends App {

  implicit val actorSystem: ActorSystem = ActorSystem("kafka-consumer")
  implicit val materializer: ActorMaterializer.type = ActorMaterializer

  val consumerSettings = ConsumerSettings(actorSystem, new ByteArrayDeserializer, new StringDeserializer)

  Consumer.plainSource(consumerSettings, Subscriptions.topics(KafkaTopics.TestTopic))
    .mapAsync(1) (consumerRecord => {
      val movie = consumerRecord.value().parseJson.convertTo[Movie]
      println(s"Message Received: ${consumerRecord.timestamp} - ${movie}")

      Future.successful(consumerRecord)
    }).runWith(Sink.ignore)
}
