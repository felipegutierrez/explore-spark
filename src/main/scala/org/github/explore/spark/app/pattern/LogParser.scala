package org.github.explore.spark.app.pattern

import java.util.regex.Matcher
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.github.explore.spark.util.Utils

/**
 * cd explore-spark/src/main/resources
 * nc -l 9999 < access_log.txt
 */
object LogParser {

  def main(args: Array[String]): Unit = {
    LogParser.run()
  }

  def run(): Unit = {
    run(Utils.VALUE_DEFAULT, Utils.VALUE_DEFAULT)
  }

  def run(input: String, output: String): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]") // load from conf/spark-defaults.conf
      .set("spark.plugins", "org.sense.spark.util.CustomMetricSparkPlugin")
      .setAppName(LogParser.getClass.getSimpleName)

    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = Utils.apacheLogPattern()

    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.sparkContext.setLogLevel(Level.ERROR.toString)

    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    // Extract the request field from each log line
    val requests = lines.map(x => {
      val matcher: Matcher = pattern.matcher(x);
      if (matcher.matches()) matcher.group(5)
    })

    // Extract the URL from the request
    val urls = requests.map(x => {
      val arr = x.toString().split(" ");
      if (arr.size == 3) arr(1) else "[error]"
    })

    // Reduce by URL over a 5-minute window sliding every second
    val urlCounts = urls.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))

    // Sort and print the results
    val sortedResults = urlCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    sortedResults.print()

    // Kick it off
    ssc.checkpoint("/tmp/spark/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}
