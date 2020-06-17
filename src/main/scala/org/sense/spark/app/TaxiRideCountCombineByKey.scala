package org.sense.spark.app

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}
import org.sense.spark.util.TaxiRideSource

object TaxiRideCountCombineByKey {

  def main(args: Array[String]): Unit = {

    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 4 cores to prevent from a starvation scenario.
    val sparkConf = new SparkConf()
      .setAppName("TaxiRideCountCombineByKey")
      .setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val stream = ssc.receiverStream(new TaxiRideSource())
    val driverStream = stream.map(taxiRide => (taxiRide.driverId, 1))
    val countStream = driverStream.combineByKey(
      (v) => (v, 1), //createCombiner
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1), //mergeValue
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2), // mergeCombiners
      new HashPartitioner(3)
    )
    countStream.print()

    ssc.start() // Start the computation

    ssc.awaitTermination() // Wait for the computation to terminate
  }
}
