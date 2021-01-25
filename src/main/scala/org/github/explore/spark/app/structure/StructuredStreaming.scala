package org.github.explore.spark.app.structure

import java.text.SimpleDateFormat
import java.util.Locale
import java.util.regex.{Matcher, Pattern}
import org.apache.log4j.Level
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.github.explore.spark.util.Utils
import Utils.apacheLogPattern

// Case class defining structured data for a line of Apache access log data
case class LogEntry(ip: String, client: String, user: String, dateTime: String, request: String, status: String, bytes: String, referer: String, agent: String)

object StructuredStreaming {
  val logPattern = apacheLogPattern()
  val datePattern = Pattern.compile("\\[(.*?) .+]")

  def main(args: Array[String]): Unit = {
    StructuredStreaming.run()
  }

  def run(): Unit = {
    run(Utils.VALUE_DEFAULT, Utils.VALUE_DEFAULT)
  }

  def run(input: String, output: String): Unit = {
    val spark = SparkSession
      .builder
      .appName(StructuredStreaming.getClass.getSimpleName)
      .master("local[*]")
      // .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .config("spark.sql.streaming.checkpointLocation", "/tmp/spark/checkpoint/")
      .getOrCreate()
    spark.sparkContext.setLogLevel(Level.ERROR.toString)

    // Create a stream of text files dumped into the logs directory
    val rawData: DataFrame = spark.readStream.text("/tmp/logs")

    // Must import spark.implicits for conversion to DataSet to work!
    import spark.implicits._

    // Convert our raw text into a DataSet of LogEntry rows, then just select the two columns we care about
    val structuredData: DataFrame = rawData.flatMap(parseLog).select("status", "dateTime")

    // Group by status code, with a one-hour window.
    val windowed: DataFrame = structuredData.groupBy($"status", window($"dateTime", "1 hour")).count().orderBy("window")

    // Start the streaming query, dumping results to the console. Use "complete" output mode because we are aggregating
    // (instead of "append").
    val query: StreamingQuery = windowed.writeStream.outputMode("complete").format("console").start()

    // Keep going until we're stopped.
    query.awaitTermination()

    spark.stop()
  }

  // Convert a raw line of Apache access log data to a structured LogEntry object (or None if line is corrupt)
  def parseLog(x: Row): Option[LogEntry] = {

    val matcher: Matcher = logPattern.matcher(x.getString(0));
    if (matcher.matches()) {
      val timeString = matcher.group(4)
      return Some(LogEntry(
        matcher.group(1),
        matcher.group(2),
        matcher.group(3),
        parseDateField(matcher.group(4)).getOrElse(""),
        matcher.group(5),
        matcher.group(6),
        matcher.group(7),
        matcher.group(8),
        matcher.group(9)
      ))
    } else {
      return None
    }
  }

  // Function to convert Apache log times to what Spark/SQL expects
  def parseDateField(field: String): Option[String] = {

    val dateMatcher = datePattern.matcher(field)
    if (dateMatcher.find) {
      val dateString = dateMatcher.group(1)
      val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
      val date = (dateFormat.parse(dateString))
      val timestamp = new java.sql.Timestamp(date.getTime());
      return Option(timestamp.toString())
    } else {
      None
    }
  }
}
