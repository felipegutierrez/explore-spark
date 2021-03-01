package org.github.explore.spark.sql

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.funsuite.AnyFunSuite

class SparkSqlSpec extends AnyFunSuite with SharedSparkContext {

  test("we should get the correct amount of cars in USA using Spark SQL") {
    val carsFromUSA = SparkSql.selectCarsFromUSA()
    carsFromUSA.show
    val count = carsFromUSA.count
    assertResult(254)(count)
  }
}
