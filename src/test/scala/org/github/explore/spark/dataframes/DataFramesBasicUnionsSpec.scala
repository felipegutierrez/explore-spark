package org.github.explore.spark.dataframes

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class DataFramesBasicUnionsSpec extends AnyFunSuite with SharedSparkContext {
  val list01 = List(
    Row("chevrolet chevelle malibu", Map("gallon" -> 18.0), 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA"),
    Row("buick skylark 320", Map("gallon" -> 15.0, "liters" -> 13.0), 8L, 350.0, 165L, 3693L, 11.5, "1970-01-01", "USA"),
    Row("plymouth satellite", Map("gallon" -> 18.0), 8L, 318.0, 150L, 3436L, 11.0, "1970-01-01", "USA")
  )
  val list02 = List(
    Row("chevrolet chevelle malibu", Map("gallon" -> 18.0, "liters" -> 15.0), 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA"),
    Row("buick skylark 320", Map("gallon" -> 15.0), 8L, 350.0, 165L, 3693L, 11.5, "1970-01-01", "USA"),
    Row("plymouth satellite", Map("gallon" -> 18.0), 8L, 318.0, 150L, 3436L, 11.0, "1970-01-01", "USA"),
    Row("amc rebel sst", Map("gallon" -> 16.0), 8L, 304.0, 150L, 3433L, 12.0, "1970-01-01", "USA")
  )

  test("two equal data frames must have all rows as intersection") {
    val df01 = DataFramesBasicUnions.createDataFrame(list01.asJava)
    val df02 = DataFramesBasicUnions.createDataFrame(list01.asJava)
    val intersectionCount = DataFramesBasicUnions.intersect(df01, df02).count()
    assertResult(3L)(intersectionCount)
  }
  test("two different data frames must have intersections on equal rows") {
    val df01 = DataFramesBasicUnions.createDataFrame(list01.asJava)
    val df02 = DataFramesBasicUnions.createDataFrame(list02.asJava)
    val intersectionCount = DataFramesBasicUnions.intersect(df01, df02).count()
    assertResult(1L)(intersectionCount)
  }
  test("two equal data frames must double the number of rows after an union transformation") {
    val df01 = DataFramesBasicUnions.createDataFrame(list01.asJava)
    val df02 = DataFramesBasicUnions.createDataFrame(list01.asJava)
    val intersectionCount = DataFramesBasicUnions.union(df01, df02).count()
    assertResult(6L)(intersectionCount)
  }
  test("two equal data frames must preserve the distinct number of rows after an union -> distinct transformation") {
    val df01 = DataFramesBasicUnions.createDataFrame(list01.asJava)
    val df02 = DataFramesBasicUnions.createDataFrame(list01.asJava)
    val intersectionCount = DataFramesBasicUnions.unionDistinct(df01, df02).count()
    assertResult(3L)(intersectionCount)
  }
  test("two different data frames must preserve the distinct number of rows after an union -> distinct transformation") {
    val df01 = DataFramesBasicUnions.createDataFrame(list01.asJava)
    val df02 = DataFramesBasicUnions.createDataFrame(list02.asJava)
    val intersectionCount = DataFramesBasicUnions.unionDistinct(df01, df02).count()
    assertResult(6L)(intersectionCount)
  }
}
