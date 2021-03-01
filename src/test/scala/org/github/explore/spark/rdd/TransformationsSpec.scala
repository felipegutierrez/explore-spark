package org.github.explore.spark.rdd

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.funsuite.AnyFunSuite

import scala.util.{Failure, Success}

class TransformationsSpec extends AnyFunSuite with SharedSparkContext {

  test("process a filtering transformation in a file that exists") {
    val result = Transformations.filtering("src/main/resources/data/stocks.csv", "MSFT")
    result match {
      case Success(value) =>
        println(s"result: $value")
        assertResult(123)(value)
      case Failure(exception) =>
        fail(s"exception: $exception")
    }
  }

  test("process a filtering transformation in a file that does not exists") {
    val result = Transformations.filtering("src/main/resources/data/does_not_exist.csv", "MSFT")
    result match {
      case Success(value) =>
        fail(s"result should fail: $value")
      case Failure(exception) =>
        assert(exception.getMessage.contains("No such file or directory"))
    }
  }

  test("using ordering to get min stock") {
    val stock = Transformations.getComparison()
    println(stock)
    assertResult(5.97)(stock.price)
  }

  test("repartition the RDD") {
    val result = Transformations.getRepartition()
  }

  test("read movies and show distinct genre") {
    val result = Transformations.distinctGenre()
    result.foreach(println(_))
  }

  test("Select all the movies in the Drama genre with IMDB rating > 6.") {
    val result = Transformations.getAllMoviesWithRatingGreaterThan(6)
    result.foreach(println(_))
    val count = result.count
    assertResult(591)(count)
  }

  test("Show the average rating of movies by genre.") {
    val result = Transformations.getAvgByGenre()
    result.foreach(println(_))
  }
}
