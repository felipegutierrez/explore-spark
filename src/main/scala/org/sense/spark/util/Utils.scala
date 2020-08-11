package org.sense.spark.util

import java.util.regex.Pattern

object Utils {
  val PARAMETER_APP: String = "-app"
  val PARAMETER_INPUT: String = "-input"
  val PARAMETER_OUTPUT: String = "-output"
  val PARAMETER_COUNT: String = "-count"
  val PARAMETER_MASTER: String = "-master"

  val VALUE_DEFAULT: String = "default"
  val VALUE_MQTT: String = "mqtt"
  val VALUE_FILE: String = "file"
  val VALUE_KAFKA: String = "kafka"
  val VALUE_MAX_COUNT: Int = 0
  val VALUE_MASTER: String = "local[*]"

  val TOPIC_KAFKA: String = "topic-kafka"
  val TOPIC_KAFKA_TAXI_RIDE: String = "topic-kafka-taxi-ride"
  val TOPIC_MQTT_SINK: String = "spark-mqtt-sink"

  /** Retrieves a regex Pattern for parsing Apache access logs. */
  def apacheLogPattern(): Pattern = {
    val ddd = "\\d{1,3}"
    val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"
    val client = "(\\S+)"
    val user = "(\\S+)"
    val dateTime = "(\\[.+?\\])"
    val request = "\"(.*?)\""
    val status = "(\\d{3})"
    val bytes = "(\\S+)"
    val referer = "\"(.*?)\""
    val agent = "\"(.*?)\""
    val regex = s"$ip $client $user $dateTime $request $status $bytes $referer $agent"
    Pattern.compile(regex)
  }
}
