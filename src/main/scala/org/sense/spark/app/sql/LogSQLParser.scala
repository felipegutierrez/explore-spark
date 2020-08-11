package org.sense.spark.app.sql

import java.util.regex.Matcher

import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.sense.spark.util.Utils

/** Case class for converting RDD to DataFrame */
case class Record(url: String, status: Int, agent: String)

/**
 * cd explore-spark/src/main/resources
 * nc -l 9999 < access_log.txt
 */
object LogSQLParser {
  def main(args: Array[String]): Unit = {
    LogSQLParser.run()
  }

  def run(): Unit = {
    run(Utils.VALUE_DEFAULT, Utils.VALUE_DEFAULT)
  }

  def run(input: String, output: String): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]") // load from conf/spark-defaults.conf
      .set("spark.sql.warehouse.dir", "/tmp/spark/sql/warehouse")
      .setAppName(LogSQLParser.getClass.getSimpleName)
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.sparkContext.setLogLevel(Level.ERROR.toString)

    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = Utils.apacheLogPattern()

    // Create a socket stream to read log data published via netcat on port 9999 locally
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    // Extract the (URL, status, user agent) we want from each log line
    val requests = lines.map(x => {
      val matcher: Matcher = pattern.matcher(x)
      if (matcher.matches()) {
        val request = matcher.group(5)
        val requestFields = request.toString().split(" ")
        val url = util.Try(requestFields(1)) getOrElse "[error]"
        (url, matcher.group(6).toInt, matcher.group(9))
      } else {
        ("error", 0, "error")
      }
    })

    // Process each RDD from each batch as it comes in
    requests.foreachRDD((rdd, time) => {
      // So we'll demonstrate using SparkSQL in order to query each RDD
      // using SQL queries.

      val spark = SparkSession
        .builder()
        .appName(LogSQLParser.getClass.getSimpleName)
        .getOrCreate()

      import spark.implicits._

      // SparkSQL can automatically create DataFrames from Scala "case classes".
      // We created the Record case class for this purpose.
      // So we'll convert each RDD of tuple data into an RDD of "Record"
      // objects, which in turn we can convert to a DataFrame using toDF()
      val requestsDataFrame = rdd.map(w => Record(w._1, w._2, w._3)).toDF()

      // Create a SQL table from this DataFrame
      requestsDataFrame.createOrReplaceTempView("requests")

      // Count up occurrences of each user agent in this RDD and print the results.
      // The powerful thing is that you can do any SQL you want here!
      // But remember it's only querying the data in this RDD, from this batch.
      val wordCountsDataFrame =
      spark.sqlContext.sql("select agent, count(*) as total from requests group by agent")
      println(s"========= $time =========")
      wordCountsDataFrame.show()

      // If you want to dump data into an external database instead, check out the
      // org.apache.spark.sql.DataFrameWriter class! It can write dataframes via
      // jdbc and many other formats! You can use the "append" save mode to keep
      // adding data from each batch.
    })

    // Kick it off
    ssc.checkpoint("/tmp/spark/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}
