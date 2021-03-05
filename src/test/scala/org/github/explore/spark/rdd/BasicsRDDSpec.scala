package org.github.explore.spark.rdd

import org.github.explore.spark.SharedSparkSession
import org.scalatest.funsuite.AnyFunSuite

import scala.util.{Failure, Success}

class BasicsRDDSpec extends AnyFunSuite with SharedSparkSession {
  test("calculate an arithmetic progression using RDDs") {
    val begin: Double = 1
    val end: Double = 1000000
    // the arithmetic progression
    val expected = end * ((begin + end) / 2)

    val value = BasicsRDD.getCollectionParallelizedAndSum(begin.toLong, end.toLong)

    value match {
      case Success(v) =>
        println(s"valid number: $v - expected: $expected")
        assertResult(expected)(value.getOrElse(0.0))
      case Failure(exception) =>
        println(s"invalid number ${exception.getMessage}");
        fail(s"invalid number ${exception.getMessage}")
    }
  }
  test("read RDD from an existing file") {
    val result = BasicsRDD.readRDDFromFile("src/main/resources/data/stocks.csv")
    result match {
      case Success(v) =>
        val count = v.count
        assertResult(560)(count)
      case Failure(exception) =>
        fail(s"invalid ${exception.getMessage}")
    }
  }
  test("read RDD from a non existing file") {
    val result = BasicsRDD.readRDDFromFile("src/main/resources/data/this_file_does_not_exist.csv")
    result match {
      case Success(v) =>
        val count = v.count
        assertResult(0)(count)
      case Failure(exception) =>
        fail(s"invalid ${exception.getMessage}")
    }
  }
  test("read RDD from a DataFrame using an existing file") {
    val result = BasicsRDD.readFromDataFrame("src/main/resources/data/stocks.csv")
    result match {
      case Success(v) =>
        val count = v.count
        assertResult(560)(count)
      case Failure(exception) =>
        fail(s"invalid ${exception.getMessage}")
    }
  }
  test("read RDD from a DataFrame using a non existing file") {
    val result = BasicsRDD.readFromDataFrame("src/main/resources/data/this_file_does_not_exist.csv")
    result match {
      case Success(v) =>
        fail(s"File $v does not exist")
      case Failure(exception) =>
        assert(exception.getMessage.contains("Path does not exist"))
    }
  }
}
