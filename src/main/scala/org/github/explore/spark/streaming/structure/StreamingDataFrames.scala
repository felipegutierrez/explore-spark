package org.github.explore.spark.streaming.structure

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.github.explore.spark.common.stocksSchema

import scala.concurrent.duration.DurationInt

object StreamingDataFrames {

  def main(args: Array[String]): Unit = {
    run()
  }

  def run(): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(StreamingDataFrames.getClass.getSimpleName)
      .master("local[2]")
      .getOrCreate()

    // val lines = readData(spark, "socket")
    val lines = readData(spark, "csv")
    val streamingQuery = writeData(lines)
    // every 2 seconds run the query
    // val streamingQuery = writeDataWithTrigger(lines, trigger = Trigger.ProcessingTime(2.seconds))
    // single batch, then terminate
    // val streamingQuery = writeDataWithTrigger(lines, trigger = Trigger.Once())
    // experimental, every 2 seconds create a batch with whatever you have
    // val streamingQuery = writeDataWithTrigger(lines, trigger = Trigger.Continuous(2.seconds))
    streamingQuery.awaitTermination()
  }

  def readData(spark: SparkSession, source: String = "socket"): DataFrame = {
    // reading a DF
    source match {
      case "socket" =>
        val lines: DataFrame = spark.readStream
        .format(source)
        .option("host", "localhost")
        .option("port", 12345)
        .load()
        lines
      case "csv" =>
        val stocksDF: DataFrame = spark.readStream
          .format("csv")
          .option("header", "false")
          .option("dateFormat", "MMM d yyyy")
          .schema(stocksSchema)
          .load("src/main/resources/data/stocks")
        stocksDF
    }
  }

  def writeData(df: DataFrame, sink: String = "console", queryName: String = "calleventaggs", outputMode: String = "append"): StreamingQuery = {

    // tell between a static vs a streaming DF
    println(s"Is this a streaming data frame: ${df.isStreaming}")

    // transformation
    val shortLines: DataFrame = df.filter(functions.length(col("value")) >= 3)

    // consuming a DF
    val query = shortLines.writeStream
      .format(sink)
      .queryName(queryName)
      .outputMode(outputMode)
      .start()
    query
  }

  def writeDataWithTrigger(df: DataFrame,
                           sink: String = "console",
                           queryName: String = "calleventaggs",
                           outputMode: String = "append",
                           trigger: Trigger = Trigger.ProcessingTime(2.seconds)): StreamingQuery = {

    // tell between a static vs a streaming DF
    println(s"Is this a streaming data frame: ${df.isStreaming}")

    // transformation
    val shortLines: DataFrame = df.filter(functions.length(col("value")) >= 3)

    // consuming a DF
    val query = shortLines.writeStream
      .format(sink)
      .queryName(queryName)
      .outputMode(outputMode)
      .trigger(trigger)
      .start()
    query
  }
}
