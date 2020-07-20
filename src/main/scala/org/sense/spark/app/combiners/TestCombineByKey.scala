package org.sense.spark.app.combiners

import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * Sample for using PairRDDFunctions.combineByKey(...)
 */

// Data store class for student and their subject score
case class ScoreDetail(studentName: String, subject: String, score: Float)

object TestCombineByKey {
  def main(args: Array[String]): Unit = {

    val scores = List(ScoreDetail("A", "Math", 98), ScoreDetail("A", "English", 88),
      ScoreDetail("B", "Math", 75), ScoreDetail("B", "English", 78),
      ScoreDetail("C", "Math", 90), ScoreDetail("C", "English", 80),
      ScoreDetail("D", "Math", 91), ScoreDetail("D", "English", 80))

    // convert to (key, values) -> (Student Name: String, score: ScoreDetail)
    val scoresWithKey = for {i <- scores} yield (i.studentName, i)

    val sparkConf = new SparkConf().setAppName("TestCombineByKey")
      .setMaster("local[3]")
      .set("spark.executor.memory", "1g")

    val sc = new SparkContext(sparkConf)
    // val sc = new StreamingContext(sparkConf, Seconds(1))

    // If data set is reused then cache recommended...
    val scoresWithKeyRDD = sc.parallelize(scoresWithKey).partitionBy(new HashPartitioner(3)).cache

    println(">>>> Number of partitions: " + scoresWithKeyRDD.getNumPartitions)

    println(">>>> Elements in each partition")

    scoresWithKeyRDD.foreachPartition(partition => println(partition.length))

    // explore each partition...
    println(">>>> Exploring partitions' data...")

    scoresWithKeyRDD.foreachPartition(
      partition => partition.foreach(
        item => println(item._2)))

    // Combine the scores for each student
    val avgScoresRDD = scoresWithKeyRDD.combineByKey(
      (x: ScoreDetail) => (x.score, 1) /*createCombiner*/ ,
      (acc: (Float, Int), x: ScoreDetail) => (acc._1 + x.score, acc._2 + 1) /*mergeValue*/ ,
      (acc1: (Float, Int), acc2: (Float, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2) /*mergeCombiners*/
      // calculate the average
    ).map({ case (key, value) => (key, value._1 / value._2) })

    avgScoresRDD
      .collect
      .foreach(println)
  }
}
