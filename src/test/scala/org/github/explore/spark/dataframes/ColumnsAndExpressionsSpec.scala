package org.github.explore.spark.dataframes

import org.apache.spark.sql
import org.apache.spark.sql.functions.col
import org.github.explore.spark.SharedSparkSession
import org.scalatest.funsuite.AnyFunSuite

class ColumnsAndExpressionsSpec extends AnyFunSuite with SharedSparkSession {

  import ColumnsAndExpressions._

  test("there is only 3 countries in the car data frame") {
    val carsDF: sql.DataFrame = getDataFrameFromJson("src/main/resources/data/cars.json")

    val allCountries = getDataFrameWithDistinct(carsDF, "Origin")
    val expectedCountries = List("[Europe]", "[Japan]", "[USA]")

    allCountries.foreach { c =>
      assert(expectedCountries.contains(c.toString()))
      println(c)
    }
    assertResult(3)(allCountries.count())
  }

  /**
   * Exercises
   *
   * 1. Read the movies DF and select 2 columns of your choice
   * 2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales
   * 3. Select all COMEDY movies with IMDB rating above 6
   *
   * Use as many versions as possible
   */
  test("Read the movies DF and select 2 columns of your choice") {
    val moviesDF: sql.DataFrame = getDataFrameFromJson("src/main/resources/data/movies.json")
    val moviesDFWithTwoColumns = moviesDF.select("Title", "Major_Genre")
    val columnsExpect: Array[String] = Array("Title", "Major_Genre")
    val columns: Array[String] = moviesDFWithTwoColumns.columns
    assertResult(columnsExpect)(columns)
  }
  test("Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales") {
    val moviesDF: sql.DataFrame = getDataFrameFromJson("src/main/resources/data/movies.json")
    val moviesDFWithAnotherColumn = moviesDF.select(
      col("Title"),
      col("Major_Genre"),
      col("US_Gross"),
      col("Worldwide_Gross"),
      col("US_DVD_Sales"),
      (col("US_Gross") + col("Worldwide_Gross")).as("Total_Gross")
    )
    val columnsExpect: Array[String] = Array("Title", "Major_Genre", "US_Gross", "Worldwide_Gross", "US_DVD_Sales", "Total_Gross")
    val columns: Array[String] = moviesDFWithAnotherColumn.columns
    moviesDFWithAnotherColumn.printSchema()
    assertResult(columnsExpect)(columns)
  }
  test("Select all COMEDY movies with IMDB rating above 6") {
    val moviesDF: sql.DataFrame = getDataFrameFromJson("src/main/resources/data/movies.json")
    val moviesDFWithAnotherColumn = moviesDF.select(
      col("Title"),
      col("Major_Genre"),
      col("US_Gross"),
      col("Worldwide_Gross"),
      col("US_DVD_Sales"),
      col("IMDB_Rating"))
      .filter(col("Major_Genre") === "Comedy" or col("Major_Genre") === "Romantic Comedy")
      .filter(col("IMDB_Rating") > 6)
    assertResult(357)(moviesDFWithAnotherColumn.count())
    moviesDFWithAnotherColumn.show()
  }
}
