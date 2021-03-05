package org.github.explore.spark.dataframes

import org.apache.spark.sql.functions.col
import org.github.explore.spark.SharedSparkSession
import org.scalatest.funsuite.AnyFunSuite

class AggregationsAndGroupingSpec extends AnyFunSuite with SharedSparkSession {

  /**
   * Exercises
   *
   * 1. Sum up ALL the profits of ALL the movies in the DF
   * 2. Count how many distinct directors we have
   * 3. Show the mean and standard deviation of US gross revenue for the movies
   * 4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
   */

  import AggregationsAndGrouping._

  test("Sum up ALL the profits of ALL the movies in the DF") {
    val value: Long = sumAllProfits().first().getLong(0)
    assertResult(413129480065L)(value)
  }
  test("Count how many distinct directors we have") {
    val value = countDistinctMovies("Director").first().getLong(0)
    assertResult(550L)(value)
  }
  test("Show the mean and standard deviation of US gross revenue for the movies") {
    val df = averageWithStdDev("US_Gross")
    df.printSchema()
    df.show()
    val mean = df.first().getDouble(0)
    val stdDev = df.first().getDouble(1)
    assert(mean > 4.0002080E7)
    assert(mean < 4.5002085E7)
    assert(stdDev > 6.05553113E7)
    assert(stdDev < 6.55553113E7)
  }
  test("Compute the average IMDB rating and the average US gross revenue PER DIRECTOR") {
    val avgDF = groupByAvgAndSum("Director", "IMDB_Rating", "US_Gross")
      .orderBy(col("IMDB_Rating").desc_nulls_last)
    avgDF.show()

  }
}
