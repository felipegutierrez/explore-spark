package org.sense.spark.app.combiners

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}

import scala.collection.mutable.Queue

object WordCountStreamCombineByKey {
  def run(): Unit = {
    // StreamingExamples.setStreamingLogLevels()

    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 4 cores to prevent from a starvation scenario.
    val sparkConf = new SparkConf()
      .setAppName("QueueStreamWordCount")
      .setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val rddQueue = new Queue[RDD[String]]()
    val lines = ssc.queueStream(rddQueue).cache()
    // val lines = ssc.socketTextStream("localhost", 9999)

    // val streamOfWords = lines.flatMap(_.split(" "))
    val streamOfWords = lines.flatMap { l => l.split(" ") }

    // UDF
    val wordTuples = (word: String) => (word, 1)
    val streamOfTuples = streamOfWords.map(wordTuples)

    // Combiner UDFs
    val combiner = (v: Int) => v
    val combinerMergeValue = (acc: Int, v: Int) => acc + v
    val combinerMergeCombiners = (acc1: Int, acc2: Int) => acc1 + acc2
    val wordCounts = streamOfTuples.combineByKey(combiner, combinerMergeValue, combinerMergeCombiners, new HashPartitioner(4))

    // val wordCounts = streamOfTuples.reduceByKey(_ + _)
    // val wordCountsWindow = wordCounts.window(Seconds(10))

    wordCounts.print()

    // Start the computation
    ssc.start()

    // Create and push some RDDs into the queue
    val thread = new Thread("pool data source") {
      override def run() {
        while (true) {
          rddQueue.synchronized {
            rddQueue += ssc.sparkContext.makeRDD(List("to be or not to be , that is the question , or what would be the question ?"))
          }
          Thread.sleep(100)
        }
      }
    }
    thread.start()

    // Wait for the computation to terminate
    ssc.awaitTermination()
  }
}
