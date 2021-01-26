package org.github.explore.scala

import org.scalatest.funsuite.AnyFunSuite

class IntToBolleanConversionSpec extends AnyFunSuite {
  // one random map
  val map0: Map[Int, Int] = Map((120 -> 1), (121 -> 1), (122 -> 0), (123 -> 0))
  // two maps that suppose to be equal
  val map1: Map[Int, Int] = Map((120 -> 1), (121 -> 1), (122 -> 0), (123 -> 0), (124 -> 1))
  val map2: Map[Int, Boolean] = Map((120 -> true), (121 -> true), (122 -> false), (123 -> false), (124 -> true))

  def mapIntToBoolean(m1: Map[Int, Int]): Map[Int, Boolean] = {
    m1.map { v =>
      val newValue = {
        if (v._2 == 1) true
        else if (v._2 == 0) false
        else {
          new RuntimeException("value is not 1 or 0")
          false
        }
      }
      (v._1 -> newValue)
    }
  }

  test("test map intToBoolean converts correctly") {

    val map3: Map[Int, Boolean] = mapIntToBoolean(map1)
    map3.foreach(println)
    val diff: Map[Int, Boolean] = (map2.toSet diff map3.toSet).toMap
    println("difference must be 0")
    assertResult(0L)(diff.size)

    val map4: Map[Int, Boolean] = mapIntToBoolean(map0)
    map4.foreach(println)
    val diff1: Map[Int, Boolean] = (map2.toSet diff map4.toSet).toMap
    println("difference must be 1")
    assertResult(1L)(diff1.size)
  }
}
