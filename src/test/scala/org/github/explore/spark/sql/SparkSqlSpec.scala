package org.github.explore.spark.sql

import org.github.explore.spark.SharedSparkSession
import org.scalatest.funsuite.AnyFunSuite

class SparkSqlSpec extends AnyFunSuite with SharedSparkSession {

  test("we should get the correct amount of cars in USA using Spark SQL") {
    val carsFromUSA = SparkSql.selectCarsFromUSA()
    carsFromUSA.show
    val count = carsFromUSA.count
    assertResult(254)(count)
  }
}
