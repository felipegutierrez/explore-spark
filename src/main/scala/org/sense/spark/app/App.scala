package org.sense.spark.app

import org.sense.spark.app.combiners.{TaxiRideAvgCombineByKey, TaxiRideCountCombineByKey, WordCountStreamCombineByKey}
import org.sense.spark.app.tests.CustomMetricExample

object App {
  def main(args: Array[String]): Unit = {
    if (args.length >= 2) {
      val param: String = args(0)
      val app: Int = args(1).toInt
      app match {
        case 1 => WordCountStreamCombineByKey.run()
        case 2 => TaxiRideCountCombineByKey.run("mqtt")
        case 3 => TaxiRideAvgCombineByKey.run("mqtt")
        case 4 => CustomMetricExample.run()
        case _ => println("Invalid application.")
      }
    } else {
      println("Please run the program and specify an application to launch at the Spark cluster.")
      println("org.sense.spark.app.App -app 1: WordCountStreamCombineByKey")
      println("-app 2: TaxiRideCountCombineByKey")
      println("-app 3: TaxiRideAvgCombineByKey")
      println("-app 4: CustomMetricExample")
    }
  }
}
