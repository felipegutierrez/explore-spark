package org.github.explore

import org.github.explore.spark.app.combiners.{TaxiRideAvgCombineByKey, TaxiRideCountCombineByKey, WordCountStreamCombineByKey}
import org.github.explore.spark.app.pattern.LogParser
import org.github.explore.spark.app.sql.LogSQLParser
import org.github.explore.spark.app.structure.StructuredStreaming
import org.github.explore.spark.app.tests.CustomMetricExample
import org.github.explore.spark.kafka.TaxiRideKafkaProducer
import org.github.explore.spark.util.Utils

object App {

  def main(args: Array[String]): Unit = {

    if (args != null && args.length > 0) {
      var i: Int = 0
      var app: Int = 0
      var maxCount: Int = Utils.VALUE_MAX_COUNT
      var input: String = Utils.VALUE_DEFAULT
      var output: String = Utils.VALUE_MQTT
      var master: String = Utils.VALUE_MASTER
      while (i < args.length) {
        if (Utils.PARAMETER_APP.equals(args(i))) {
          i += 1
          app = args(i).toInt
        } else if (Utils.PARAMETER_OUTPUT.equals(args(i))) {
          i += 1
          output = args(i)
        } else if (Utils.PARAMETER_INPUT.equals(args(i))) {
          i += 1
          input = args(i)
        } else if (Utils.PARAMETER_COUNT.equals(args(i))) {
          i += 1
          maxCount = args(i).toInt
        } else if (Utils.PARAMETER_MASTER.equals(args(i))) {
          i += 1
          master = args(i)
        }
        i += 1
      }
      if (args.length >= 2) {
        app match {
          case 1 => WordCountStreamCombineByKey.run()
          case 2 => TaxiRideCountCombineByKey.run(input, output)
          case 3 => TaxiRideAvgCombineByKey.run(input, output)
          case 4 => CustomMetricExample.run()
          case 5 => new TaxiRideKafkaProducer(maxCount)
          case 6 => LogParser.run(input, output)
          case 7 => LogSQLParser.run(input, output)
          case 8 => StructuredStreaming.run(input, output)
          case _ => println("Invalid application.")
        }
      } else {
        println("Please run the program and specify an application to launch at the Spark cluster.")
        println("org.sense.spark.app.App -app 1: WordCountStreamCombineByKey")
        println("-app 2: " + TaxiRideCountCombineByKey.getClass.getSimpleName)
        println("-app 3: " + TaxiRideAvgCombineByKey.getClass.getSimpleName)
        println("-app 4: " + CustomMetricExample.getClass.getSimpleName)
        println("-app 5: TaxiRideKafkaProducer")
        println("-app 6: " + LogParser.getClass.getSimpleName)
        println("-app 7: " + LogSQLParser.getClass.getSimpleName)
        println("-app 8: " + StructuredStreaming.getClass.getSimpleName)
      }
    }
  }
}
