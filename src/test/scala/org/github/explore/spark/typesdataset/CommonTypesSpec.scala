package org.github.explore.spark.typesdataset

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite

class CommonTypesSpec extends AnyFunSuite with SharedSparkContext {

  test("we should be able to add a column on a data frame") {
    val moviesDFWithNewColumn = CommonTypes.addPlainValueColumn("plain_value")
    val columnsArray = moviesDFWithNewColumn.columns
    moviesDFWithNewColumn.printSchema()
    assert(columnsArray.contains("plain_value"))
  }
  test("evaluating a filter as a boolean column") {
    val moviesDFWithNewColumn = CommonTypes.evaluateBooleansAsColumns()
    val columnsArray = moviesDFWithNewColumn.columns
    moviesDFWithNewColumn.show()
    assert(columnsArray.contains("good_movie"))
  }
  test("show only good movies") {
    val onlyGoodMovies = CommonTypes.onlyGoodMovies()
    onlyGoodMovies.show()
    val total = onlyGoodMovies.where(col("good_movie") =!= "true").count()
    assertResult(0)(total)
  }
  test("show only bad movies") {
    val onlyBadMovies = CommonTypes.onlyBadMovies()
    onlyBadMovies.show()
    val total = onlyBadMovies.where(col("good_movie") === "true").count()
    assertResult(0)(total)
  }
  test("the pearson correlation myst be between -1 ad 1") {
    val corr = CommonTypes.getCorrelationOf("Rotten_Tomatoes_Rating", "IMDB_Rating")
    println(s"Pearson Correlation Coefficient: $corr")
    assert(corr >= -1)
    assert(corr <= 1)
  }
  test("capitalize all titles") {
    val moviesCapitalized = CommonTypes.getCorrelationOf()
    val row = moviesCapitalized.select(col("upper_title")).head()
    println(row)
    val charArray = row
      .toString()
      .replace("[", "")
      .replace("]", "")
      .replace(" ", "")
      .toCharArray
    charArray.foreach { c =>
      // println(c + " " + c.isUpper)
      assert(c.isUpper)
    }
  }
  test("using regex expression") {
    val vwCarDF = CommonTypes.usingRegexOnDataframes("volkswagen|vw")
    vwCarDF.show()
    val row = vwCarDF.take(1).head.toString()
    assert((row.contains("volkswagen") || row.contains("vw")))
  }
  test("using regex expression to replace") {
    val vwCarDF = CommonTypes.usingRegexToReplaceOnDataframes("volkswagen|vw")
    vwCarDF.show()
    val row = vwCarDF.take(1).head.toString()
    assert(row.contains("People"))
  }
  test("get cars with regex parameter from a list") {
    val carsDF = CommonTypes.getCarDfWithRegex(List("Volkswagen", "Mercedes-Benz", "Ford"))
    carsDF.show()
    val row = carsDF.take(1).head.toString()
    assert(row.contains("Volkswagen".toLowerCase()) || row.contains("Mercedes-Benz".toLowerCase()) || row.contains("Ford".toLowerCase()))
  }
  test("get cars with contains parameter from a list") {
    val carsDF = CommonTypes.getCarDfWithContain(List("Volkswagen", "Mercedes-Benz", "Ford"))
    carsDF.show()
    val row = carsDF.take(1).head.toString()
    assert(row.contains("Volkswagen".toLowerCase()) || row.contains("Mercedes-Benz".toLowerCase()) || row.contains("Ford".toLowerCase()))
  }
}
