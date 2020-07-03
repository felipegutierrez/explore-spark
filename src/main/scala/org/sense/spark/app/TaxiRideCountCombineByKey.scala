package org.sense.spark.app

import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}
import org.fusesource.mqtt.client.QoS
import org.sense.spark.util.{MqttSink, TaxiRide, TaxiRideSource}

object TaxiRideCountCombineByKey {

  val mqttTopic: String = "spark-mqtt-sink"
  val host: String = "127.0.0.1";
  val qos: QoS = QoS.AT_LEAST_ONCE

  def main(args: Array[String]): Unit = {

    val outputMqtt: Boolean = if (args.length > 0 && args(0).equals("mqtt")) true else false

    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 4 cores to prevent from a starvation scenario.
    val sparkConf = new SparkConf()
      .setAppName("TaxiRideCountCombineByKey")
      .setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Milliseconds(1000))

    val stream = ssc.receiverStream(new TaxiRideSource()).cache()

    val driverIdTaxiRide = (taxiRide: TaxiRide) => (taxiRide.driverId, 1)
    val driverStream = stream.map(driverIdTaxiRide)

    val combiner = (v: Int) => v
    val combinerMergeValue = (acc: Int, v: Int) => acc + v
    val countStream = driverStream.combineByKey(combiner, combinerMergeValue, combinerMergeValue, new HashPartitioner(4))

    if (outputMqtt) {
      println("Use the command below to consume data:")
      println("mosquitto_sub -h " + host + " -p 1883 -t " + mqttTopic)

      val mqttSink = ssc.sparkContext.broadcast(MqttSink(host))
      countStream.foreachRDD { rdd =>
        rdd.foreach { message =>
          mqttSink.value.send(mqttTopic, message.toString())
        }
      }
    } else {
      countStream.print()
    }
    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}
