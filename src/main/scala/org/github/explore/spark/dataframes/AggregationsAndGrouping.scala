package org.github.explore.spark.dataframes

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggregationsAndGrouping {
  val spark = SparkSession.builder()
    .appName(AggregationsAndGrouping.getClass.getSimpleName)
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF: sql.DataFrame = getDataFrameFromJson("src/main/resources/data/movies.json")

  def main(args: Array[String]): Unit = {
    run()
  }

  def run() = {
    countMovies().show()
    countDistinctMovies("Major_Genre").show()
    countDistinctApproxMovies("Major_Genre").show()
    average().show()
    averageWithStdDev("Rotten_Tomatoes_Rating").show()
    val countDF = groupByCount("Major_Genre")
    val sumDF = groupBySum("Major_Genre", "IMDB_Rating")
    val avgDF = groupByAvg("Major_Genre", "IMDB_Rating")

    countDF.show()
    sumDF.show()
    avgDF.show()
  }

  def countMovies(): sql.DataFrame = {
    moviesDF.select(count("*"))
  }

  def countDistinctMovies(column: String): sql.DataFrame = {
    moviesDF.select(countDistinct(col(column)))
  }

  def countDistinctApproxMovies(column: String): sql.DataFrame = {
    moviesDF.select(approx_count_distinct(col(column)))
  }

  def average(): sql.DataFrame = {
    moviesDF.select(
      avg(col("Rotten_Tomatoes_Rating"))
    )
  }

  def averageWithStdDev(column: String): sql.DataFrame = {
    moviesDF.select(
      mean(col(column)),
      stddev(col(column))
    )
  }

  def groupByCount(column: String): sql.DataFrame = {
    moviesDF.groupBy(col(column)).count()
  }

  def groupBySum(column: String, averageColumn: String): sql.DataFrame = {
    moviesDF.groupBy(col(column)).sum(averageColumn)
  }

  def groupByAvg(column: String, averageColumn: String): sql.DataFrame = {
    moviesDF.groupBy(col(column)).avg(averageColumn)
  }

  def groupByAvg(column: String, averageColumn01: String, averageColumn02: String): sql.DataFrame = {
    moviesDF.groupBy(col(column)).avg(averageColumn01, averageColumn02)
  }

  def groupByAvgAndSum(column: String, averageColumn01: String, averageColumn02: String): sql.DataFrame = {
    moviesDF
      .groupBy(col(column))
      .agg(
        avg(averageColumn01).as(averageColumn01),
        sum(averageColumn02).as(averageColumn02)
      )
  }

  def sumAllProfits() = {
    moviesDF.select((col("US_Gross") + col("Worldwide_Gross")).as("Total_Gross"))
      .select(sum("Total_Gross"))
  }

  def getDataFrameFromJson(pathJson: String): sql.DataFrame = {
    spark.read
      .option("inferSchema", "true")
      .json(pathJson)
  }
}
