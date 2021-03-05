package org.github.explore.spark.streaming.combiners

import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}
import org.fusesource.mqtt.client.QoS
import org.github.explore.spark.util.{MqttSink, TaxiRide, TaxiRideSource, Utils}

object TaxiRideAvgCombineByKey {
  val mqttTopic: String = "spark-mqtt-sink"
  val host: String = "127.0.0.1";
  val qos: QoS = QoS.AT_LEAST_ONCE

  def run(): Unit = {
    run(Utils.VALUE_DEFAULT, Utils.VALUE_DEFAULT)
  }

  def run(input: String, output: String): Unit = {

    val outputMqtt: Boolean = if ("mqtt".equals(output)) true else false

    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 4 cores to prevent from a starvation scenario.
    val sparkConf = new SparkConf()
      .setAppName(TaxiRideAvgCombineByKey.getClass.getSimpleName)
    // .setMaster("local[*]") // load from conf/spark-defaults.conf
    val ssc = new StreamingContext(sparkConf, Milliseconds(1000))

    val stream = ssc.receiverStream(new TaxiRideSource()).cache()

    // UDFs
    val driverIdTaxiRide = (taxiRide: TaxiRide) => {
      val diffInMillis: Long = taxiRide.endTime.getMillis() - taxiRide.startTime.getMillis()
      val distance: Float = TaxiRide.distance(taxiRide.startLat, taxiRide.startLon, taxiRide.endLat, taxiRide.endLon)
      (taxiRide.driverId, (diffInMillis, distance, taxiRide.passengerCnt, 1))
    }
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
      val avgMillis: Long = (acc1._1 + acc2._1)
      val avgDistance: Float = (acc1._2 + acc2._2)
      val avgPassenger = (acc1._3 + acc2._3)
      (avgMillis, avgDistance, avgPassenger.toShort, sumCount)
    }
    val averageFunction = (v: (Long, (Long, Float, Short, Int))) => {
      val avgMillis: Long = v._2._1 / v._2._4
      val avgDistance: Float = v._2._2 / v._2._4
      val avgPassenger: Float = v._2._3 / v._2._4
      (v._1, (avgMillis, avgDistance, avgPassenger, v._2._4))
    }
    val toString = (v: (Long, (Long, Float, Float, Int))) => {
      val driverId: Long = v._1
      val avgMillis: Long = v._2._1
      val avgDistance: Float = v._2._2
      val avgPassenger: Float = v._2._3
      val sumCount: Int = v._2._4
      "DriverID[" + driverId + "] AvgTripTime[" + avgMillis + "] AvgDistance[" + avgDistance + "] AvgPassengers[" + avgPassenger + "] SumEvents[" + sumCount + "]"
    }

    // map TaxiRide stream to values of distance, time trip and passengers
    val driverStream = stream.map(driverIdTaxiRide)
    // sum values of the stream using combineByKey
    val countStream = driverStream.combineByKey(combiner, combinerMergeValue, combinerMergeCombiners, new HashPartitioner(4))
    // calculate the average
    val averageResult = countStream.map(averageFunction)
    // map tuples to a string message readable
    val result = averageResult.map(toString)

    // emits the result
    if (outputMqtt) {
      println("Use the command below to consume data:")
      println("mosquitto_sub -h " + host + " -p 1883 -t " + mqttTopic)

      val mqttSink = ssc.sparkContext.broadcast(MqttSink(host))
      countStream.foreachRDD { rdd =>
        rdd.foreachPartition { partitionOfRecords =>
          partitionOfRecords.foreach(message => mqttSink.value.send(mqttTopic, message.toString()))
        }
      }
    } else {
      result.print()
    }

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}
