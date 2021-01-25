package org.github.explore.spark.util

import org.fusesource.mqtt.client.{FutureConnection, MQTT, QoS}

class MqttSink(createProducer: () => FutureConnection) extends Serializable {

  lazy val producer = createProducer()

  def send(topic: String, message: String): Unit = {
    producer.publish(topic, message.toString().getBytes, QoS.AT_LEAST_ONCE, false)
  }
}

object MqttSink {
  def apply(host: String): MqttSink = {
    val f = () => {
      val mqtt = new MQTT()
      mqtt.setHost(host, 1883)
      val producer = mqtt.futureConnection()
      producer.connect().await()
      sys.addShutdownHook {
        producer.disconnect().await()
      }
      producer
    }
    new MqttSink(f)
  }
}
