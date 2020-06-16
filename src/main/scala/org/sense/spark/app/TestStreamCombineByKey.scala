package org.sense.spark.app

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}

import scala.collection.mutable.Queue

object TestStreamCombineByKey {
  def main(args: Array[String]): Unit = {

    // StreamingExamples.setStreamingLogLevels()

    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 4 cores to prevent from a starvation scenario.
    val sparkConf = new SparkConf()
      .setAppName("QueueStreamWordCount")
      .setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val rddQueue = new Queue[RDD[String]]()
    val lines = ssc.queueStream(rddQueue)
    // val lines = ssc.socketTextStream("localhost", 9999)

    val wordCounts = lines
      .flatMap(_.split(" "))
      .map(word => (word, 1))
      // .reduceByKey(_ + _)
      .combineByKey(
        (v) => (v, 1), //createCombiner
        (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1), //mergeValue
        (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2), // mergeCombiners
        new HashPartitioner(3)
      )
    // .window(Seconds(10))

    wordCounts.print()

    ssc.start() // Start the computation

    // Create and push some RDDs into the queue
    val thread = new Thread("pool data source") {
      override def run() {
        while (true) {
          rddQueue.synchronized {
            rddQueue += ssc.sparkContext.makeRDD(List("to be or not to be , that is the question"))
          }
          Thread.sleep(100)
        }
      }
    }
    thread.start()

    ssc.awaitTermination() // Wait for the computation to terminate
  }
}
