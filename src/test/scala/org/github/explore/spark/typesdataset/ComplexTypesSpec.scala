package org.github.explore.spark.typesdataset

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite

import java.text.SimpleDateFormat
import java.util.Date

class ComplexTypesSpec extends AnyFunSuite with SharedSparkContext {

  test("parse year with to digits to the previous century if year is after 1923") {
    val date: Date = ComplexTypes.parseDate("21-Jan-24", "d-MMM-yy")
    val df = new SimpleDateFormat("yyyy")
    val year = df.format(date)
    println(year)
    assertResult("1924")(year)
  }
  test("parse year with to digits to this century if year is before or equal to 2023") {
    val date: Date = ComplexTypes.parseDate("21-Jan-23", "d-MMM-yy")
    val df = new SimpleDateFormat("yyyy")
    val year = df.format(date)
    println(year)
    assertResult("2023")(year)
  }
  test("movie age is always a positive number") {
    val moviesWithAgeDF = ComplexTypes.getMoviesWithReleaseDate()
    moviesWithAgeDF.show
    val newDF = moviesWithAgeDF
      // .select(col("Movie_Age"))
      .where(col("Movie_Age") < 0.0)
    newDF.show()
    val count = newDF.count
    assertResult(0)(count)
  }
  test("get the stock data frame and format the date") {
    val stockDF = ComplexTypes.getStocksWithReleaseDate()
    stockDF.show()
    assertResult(560)(stockDF.count)
  }
  test("movies with structures") {
    val movies = ComplexTypes.getMoviesWithStructures
    movies.show
    assertResult(Array[String]("Title", "Profit", "US_Profit"))(movies.columns)
  }
  test("movies with structures with expressions") {
    val movies = ComplexTypes.getMoviesWithStructuresUsingExpressions
    movies.show
    assertResult(Array[String]("Title", "Profit", "US_Profit"))(movies.columns)
  }
  test("movies with structures with arrays") {
    val movies = ComplexTypes.getMoviesWithStructuresUsingArrays()
    movies.show
    val count = movies.where(col("boolean") === "true").count()
    assertResult(31)(count)
  }
}
