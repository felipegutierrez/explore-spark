package org.github.explore.spark.typesdataset

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.funsuite.AnyFunSuite


class DatasetsSpec extends AnyFunSuite with SharedSparkContext {
  test("convert a data frame to a data set should allow use of scala monads") {
    val dataSetOfNumbers = Datasets.convertDataFrameToDataSet()
    dataSetOfNumbers.show
    val result = dataSetOfNumbers.filter(_ < 100).count
    assert(result > 0)
  }
  test("convert a complex data frame to a data set should allow use of scala monads") {
    val dataSetOfCars = Datasets.convertComplexDataFrameToDataSet()
    dataSetOfCars.show
    import Datasets.spark.implicits._
    val resultDataset = dataSetOfCars.map(car => car.Name.toUpperCase)
    val resultLowerCase = resultDataset.head.filter(_.isLower)
    assertResult("")(resultLowerCase)
  }
  /**
   * Exercises
   *
   * 1. Count how many cars we have
   * 2. Count how many POWERFUL cars we have (HP > 140)
   * 3. Average HP for the entire dataset
   */
  test("the cars data set should have 406 number of cars") {
    val dataSetOfCars = Datasets.convertComplexDataFrameToDataSet()
    dataSetOfCars.show
    val count = dataSetOfCars.count
    assertResult(406)(count)
  }
  test("the cars data set should have X number of cars with horse power greater than 140") {
    val dataSetOfCars = Datasets.convertComplexDataFrameToDataSet()
    val filter = dataSetOfCars
      .filter(car => car.Horsepower.getOrElse[Long](0) > 140)
    filter.show
    val count = filter.count
    println(count)
    assertResult(81)(count)
  }
  test("the average of horse power of the entire cars data set") {
    val average = Datasets.averageOfHorsePowerInCarsDataset()
    assertResult(103)(average)
  }
  test("join the datasets GuitarPlayer with Band") {
    val dataset = Datasets.joinGuitarPlayerWithBand()
    dataset.show
    val count = dataset.count
    assertResult(3)(count)
  }
  /**
   * Exercise: join the guitarsDS and guitarPlayersDS, in an outer join
   * (hint: use array_contains)
   */
  test("join the guitarsDS and guitarPlayersDS, in an outer join") {
    val dataset = Datasets.joinGuitarWithGuitarPlayer()
    dataset.show
    val count = dataset.count
    assertResult(6)(count)
  }
  test("grouping cars DS by Origin") {
    val dataset = Datasets.groupingCarsByOrigin()
    dataset.show
    val list = dataset.collectAsList()
    list.forEach { row =>
      if (row._1 == "Europe") assertResult(73)(row._2)
      else if (row._1 == "USA") assertResult(254)(row._2)
      else if (row._1 == "Japan") assertResult(79)(row._2)
    }
  }
}
