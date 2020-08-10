package org.sense.spark.util

object Utils {
  val PARAMETER_APP: String = "-app"
  val PARAMETER_INPUT: String = "-input"
  val PARAMETER_OUTPUT: String = "-output"
  val PARAMETER_COUNT: String = "-count"
  val PARAMETER_MASTER: String = "-master"

  val VALUE_DEFAULT: String = "default"
  val VALUE_MQTT: String = "mqtt"
  val VALUE_KAFKA: String = "kafka"
  val VALUE_MAX_COUNT: Int = 0
  val VALUE_MASTER: String = "local[*]"

  val TOPIC_KAFKA: String = "topic-kafka"
  val TOPIC_KAFKA_TAXI_RIDE: String = "topic-kafka-taxi-ride"
  val TOPIC_MQTT_SINK: String = "spark-mqtt-sink"
}
