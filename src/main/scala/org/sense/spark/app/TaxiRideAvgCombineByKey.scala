package org.sense.spark.app

import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}
import org.fusesource.mqtt.client.QoS
import org.sense.spark.util.{MqttSink, TaxiRide, TaxiRideSource}

object TaxiRideAvgCombineByKey {
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

    // type DriverIdTripTime = (Long, Long, Int)
    val driverIdTaxiRide = (taxiRide: TaxiRide) => {
      val diffInMillis: Long = taxiRide.endTime.getMillis() - taxiRide.startTime.getMillis()
      val distance: Float = TaxiRide.distance(taxiRide.startLat, taxiRide.startLon, taxiRide.endLat, taxiRide.endLon)
      (taxiRide.driverId, (diffInMillis, distance, taxiRide.passengerCnt, 1))
    }
    val driverStream = stream.map(driverIdTaxiRide)

    val combiner = (v: (Long, Float, Short, Int)) => v
    val combinerMergeValue = (acc: (Long, Float, Short, Int), v: (Long, Float, Short, Int)) => {
      val sumMillis: Long = acc._1 + v._1
      val sumDistance: Float = acc._2 + v._2
      val sumPassenger = acc._3 + v._3
      val sumCount: Int = acc._4 + v._4
      (sumMillis, sumDistance, sumPassenger.toShort, sumCount)
    }
    val combinerMergeCombiners = (acc1: (Long, Float, Short, Int), acc2: (Long, Float, Short, Int)) => {
      val sumCount: Int = acc1._4 + acc2._4
      val avgMillis: Long = (acc1._1 + acc2._1) / sumCount
      val avgDistance: Float = (acc1._2 + acc2._2) / sumCount
      val avgPassenger = (acc1._3 + acc2._3) / sumCount
      (avgMillis, avgDistance, avgPassenger.toShort, sumCount)
    }
    val countStream = driverStream.combineByKey(combiner, combinerMergeValue, combinerMergeCombiners, new HashPartitioner(4))

    val toString = (v: (Long, (Long, Float, Short, Int))) => {
      val driverId: Long = v._1
      val avgMillis: Long = v._2._1
      val avgDistance: Float = v._2._2
      val avgPassenger: Short = v._2._3
      val sumCount: Int = v._2._4
      "DriverID[" + driverId + "] AvgTripTime[" + avgMillis + "] AvgDistance[" + avgDistance + "] AvgPassengers[" + avgPassenger + "] SumEvents[" + sumCount + "]"
    }
    val result = countStream.map(toString)

    if (outputMqtt) {
      println("Use the command below to consume data:")
      println("mosquitto_sub -h " + host + " -p 1883 -t " + mqttTopic)

      val mqttSink = ssc.sparkContext.broadcast(MqttSink(host))
      result.foreachRDD { rdd =>
        rdd.foreach { message =>
          mqttSink.value.send(mqttTopic, message)
        }
      }
    } else {
      result.print()
    }

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}
