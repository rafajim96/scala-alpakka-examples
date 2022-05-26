package com.example.common

import com.example.models.Movie
import spray.json.DefaultJsonProtocol._
import spray.json.JsonFormat

object JsonFormats {
  implicit val movieFormat: JsonFormat[Movie] = jsonFormat3(Movie)
}
