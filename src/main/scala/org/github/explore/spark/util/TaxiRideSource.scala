package org.github.explore.spark.util

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.Locale
import java.util.zip.GZIPInputStream

import org.apache.spark.storage._
import org.apache.spark.streaming.receiver._
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

object TimeFormatter {
  val timeFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC()
}

object TaxiRideSource {
  def getTaxiRideFromString(line: String): TaxiRide = {
    // println(line)
    val tokens: Array[String] = line.split(",")
    if (tokens.length != 11) {
      throw new RuntimeException("Invalid record: " + line)
    }

    val rideId: Long = tokens(0).toLong
    val (isStart, startTime, endTime) = tokens(1) match {
      case "START" => (true, DateTime.parse(tokens(2), TimeFormatter.timeFormatter), DateTime.parse(tokens(3), TimeFormatter.timeFormatter))
      case "END" => (false, DateTime.parse(tokens(2), TimeFormatter.timeFormatter), DateTime.parse(tokens(3), TimeFormatter.timeFormatter))
      case _ => throw new RuntimeException("Invalid record: " + line)
    }
    val startLon: Float = if (tokens(4).length > 0) tokens(4).toFloat else 0.0f
    val startLat: Float = if (tokens(5).length > 0) tokens(5).toFloat else 0.0f
    val endLon: Float = if (tokens(6).length > 0) tokens(6).toFloat else 0.0f
    val endLat: Float = if (tokens(7).length > 0) tokens(7).toFloat else 0.0f
    val passengerCnt: Short = tokens(8).toShort
    val taxiId: Long = tokens(9).toLong
    val driverId: Long = tokens(10).toLong

    TaxiRide(rideId, isStart, startTime, endTime, startLon, startLat, endLon, endLat, passengerCnt, taxiId, driverId)
  }
}

class TaxiRideSource extends Receiver[TaxiRide](StorageLevel.MEMORY_AND_DISK_2) {
  val dataFilePath = "/home/flink/nycTaxiRides.gz";
  var dataRateListener: DataRateListener = _

  /**
   * Start the thread that receives data over a connection
   */
  def onStart() {
    dataRateListener = new DataRateListener()
    dataRateListener.start()
    new Thread("TaxiRide Source") {
      override def run() {
        receive()
      }
    }.start()
  }

  def onStop() {}

  /**
   * Periodically generate a TaxiRide event and regulate the emission frequency
   */
  private def receive() {
    while (!isStopped()) {
      val gzipStream = new GZIPInputStream(new FileInputStream(dataFilePath))
      val reader: BufferedReader = new BufferedReader(new InputStreamReader(gzipStream, StandardCharsets.UTF_8))
      try {
        var line: String = null
        do {
          // start time before reading the line
          val startTime = System.nanoTime

          // read the line on the file and yield the object
          line = reader.readLine
          if (line != null) {
            val taxiRide: TaxiRide = getTaxiRideFromString(line)
            store(taxiRide)
          }

          // regulate frequency of the source
          dataRateListener.busySleep(startTime)
        } while (line != null)
      } finally {
        reader.close
      }
    }
  }

  def getTaxiRideFromString(line: String): TaxiRide = {
    // println(line)
    val tokens: Array[String] = line.split(",")
    if (tokens.length != 11) {
      throw new RuntimeException("Invalid record: " + line)
    }

    val rideId: Long = tokens(0).toLong
    val (isStart, startTime, endTime) = tokens(1) match {
      case "START" => (true, DateTime.parse(tokens(2), TimeFormatter.timeFormatter), DateTime.parse(tokens(3), TimeFormatter.timeFormatter))
      case "END" => (false, DateTime.parse(tokens(2), TimeFormatter.timeFormatter), DateTime.parse(tokens(3), TimeFormatter.timeFormatter))
      case _ => throw new RuntimeException("Invalid record: " + line)
    }
    val startLon: Float = if (tokens(4).length > 0) tokens(4).toFloat else 0.0f
    val startLat: Float = if (tokens(5).length > 0) tokens(5).toFloat else 0.0f
    val endLon: Float = if (tokens(6).length > 0) tokens(6).toFloat else 0.0f
    val endLat: Float = if (tokens(7).length > 0) tokens(7).toFloat else 0.0f
    val passengerCnt: Short = tokens(8).toShort
    val taxiId: Long = tokens(9).toLong
    val driverId: Long = tokens(10).toLong

    TaxiRide(rideId, isStart, startTime, endTime, startLon, startLat, endLon, endLat, passengerCnt, taxiId, driverId)
  }
}
