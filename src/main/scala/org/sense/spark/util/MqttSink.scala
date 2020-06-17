package org.sense.spark.util

import org.fusesource.mqtt.client.{FutureConnection, MQTT}

object MqttSink {
  val mqtt = new MQTT()
  mqtt.setHost("localhost", 1883)
  val connection: FutureConnection = mqtt.futureConnection()
  connection.connect().await()
  sys.addShutdownHook {
    connection.disconnect().await()
  }

  def apply(): FutureConnection = {
    connection
  }
}
