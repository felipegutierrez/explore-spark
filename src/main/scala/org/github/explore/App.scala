package org.github.explore

import org.github.explore.spark.app.Playground
import org.github.explore.spark.app.combiners.{TaxiRideAvgCombineByKey, TaxiRideCountCombineByKey, WordCountStreamCombineByKey}
import org.github.explore.spark.app.pattern.LogParser
import org.github.explore.spark.app.sql.LogSQLParser
import org.github.explore.spark.app.structure.StructuredStreaming
import org.github.explore.spark.app.tests.CustomMetricExample
import org.github.explore.spark.dataframes.{DataFramesBasicUnions, DataFramesBasics, DataSources}
import org.github.explore.spark.kafka.TaxiRideKafkaProducer
import org.github.explore.spark.util.Utils

import java.util.Scanner

object App {

  def main(args: Array[String]): Unit = {

    println(s"0 - out")
    println(s"1 - WordCountStreamCombineByKey")
    println(s"2 - TaxiRideCountCombineByKey")
    println(s"3 - TaxiRideAvgCombineByKey")
    println(s"4 - CustomMetricExample")
    println(s"5 - TaxiRideKafkaProducer")
    println(s"6 - LogParser")
    println(s"7 - LogSQLParser")
    println(s"8 - StructuredStreaming")
    println(s"9 - Playground")
    println(s"10 - DataFramesBasics")
    println(s"11 - DataFramesComparisonBasics")
    println(s"12 - DataSources")
    println(s"13 - ")
    println(s"14 - ")
    println(s"15 - ")
    println(s"16 - ")
    println(s"17 - ")
    println(s"18 - ")
    println(s"19 - ")

    var option01: String = ""
    var option02: Int = Utils.VALUE_MAX_COUNT
    var option03: String = Utils.VALUE_DEFAULT
    var option04: String = Utils.VALUE_MQTT
    if (args.length >= 0) {
      println("choose an application: ")
      val scanner = new Scanner(System.in)
      option01 = scanner.nextLine()
      if (args.length >= 1) {
        option02 = Int.unbox(args(1))
        if (args.length >= 2) {
          option03 = args(2)
          if (args.length >= 3) {
            option04 = args(3)
          }
        }
      }
    }

    println(s"option01 (app): $option01")
    println(s"option02      : $option02")
    println(s"option03      : $option03")
    println(s"option04      : $option04")
    option01 match {
      case "0" => println(s"Bye, see you next time.")
      case "1" => WordCountStreamCombineByKey.run()
      case "2" => TaxiRideCountCombineByKey.run(option03, option04)
      case "3" => TaxiRideAvgCombineByKey.run(option03, option04)
      case "4" => CustomMetricExample.run()
      case "5" => new TaxiRideKafkaProducer(option02)
      case "6" => LogParser.run(option03, option04)
      case "7" => LogSQLParser.run(option03, option04)
      case "8" => StructuredStreaming.run(option03, option04)
      case "9" => Playground.run()
      case "10" => DataFramesBasics.run()
      case "11" => DataFramesBasicUnions.run()
      case "12" => DataSources.run()
      case "13" =>
      case "14" =>
      case "15" =>
      case "16" =>
      case "17" =>
      case "18" =>
      case "19" =>
      case _ => println("option unavailable.")
    }
  }
}
