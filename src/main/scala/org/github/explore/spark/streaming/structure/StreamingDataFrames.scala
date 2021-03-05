package org.github.explore.spark.streaming.structure

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object StreamingDataFrames {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(StreamingDataFrames.getClass.getSimpleName)
      .master("local[2]")
      .getOrCreate()

    val lines = readData(spark, "socket")
    val streamingQuery = writeData(lines)
    streamingQuery.awaitTermination()
  }

  def readData(spark: SparkSession, source: String = "socket"): DataFrame = {
    // reading a DF
    val lines: DataFrame = spark.readStream
      .format(source)
      .option("host", "localhost")
      .option("port", 12345)
      .load()
    lines
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

}
