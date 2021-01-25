package org.github.explore.spark.util

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets

import scala.util.control.Exception.allCatch

class DataRateListener extends Thread {
  val DATA_RATE_FILE = "/tmp/datarate.txt"
  var delayInNanoSeconds: Long = 1000000000

  override def run(): Unit = {
    while (true) {
      val fileName: File = new File(DATA_RATE_FILE)

      if (!fileName.exists()) {
        println("The file [" + DATA_RATE_FILE + "] does not exist. Hence the DataRate is taking the parameter equal [" + delayInNanoSeconds + "] nanoseconds.")
        println("Please use \"echo \'1000000000\' > " + DATA_RATE_FILE + "\" to create the data rate file [" + DATA_RATE_FILE + "].")
      }

      val inputBuffer = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), StandardCharsets.UTF_8))

      val line = inputBuffer.readLine()
      if (line != null) {
        println(line)
        if (isLongNumber(line)) {
          if (line.toLong > 0) {
            println("Reading new frequency to generate Taxi data: " + line + " nanoseconds.")
            delayInNanoSeconds = line.toLong
          }
        } else {
          println("ERROR: String at [" + DATA_RATE_FILE + "] is not numeric!")
        }
      }
      Thread.sleep(60 * 1000)
    }
  }

  def getDelayInNanoSeconds(): Long = {
    delayInNanoSeconds
  }

  def busySleep(startTime: Long): Unit = {
    val deadLine: Long = startTime + delayInNanoSeconds
    while (System.nanoTime() < deadLine) {}
  }

  def isLongNumber(s: String): Boolean = (allCatch opt s.toLong).isDefined
}
