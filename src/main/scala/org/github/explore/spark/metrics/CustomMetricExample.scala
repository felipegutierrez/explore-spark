package org.github.explore.spark.metrics

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.github.explore.spark.util.CustomMetricSparkPlugin

object CustomMetricExample {

  def run(): Unit = {

    val sparkConf = new SparkConf()
      .setMaster("local[4]")
      .set("spark.plugins", "org.sense.spark.util.CustomMetricSparkPlugin")
      // .set("spark.metrics.conf", "src/main/resources/metrics.properties")
      .setAppName("executor plugin example")
    val sparkSession = SparkSession.builder.config(sparkConf).getOrCreate()

    import sparkSession.implicits._

    val df = sparkSession.range(5000).repartition(5)

    val incrementedDf = df.mapPartitions(iterator => {
      var evenCount = 0
      val incrementedIterator = iterator.toList.map(value => {
        if (value % 2 == 0) evenCount = evenCount + 1
        value + 1
      }).toIterator
      CustomMetricSparkPlugin.value.inc(evenCount)
      incrementedIterator
    })
    incrementedDf.count()
  }
}
