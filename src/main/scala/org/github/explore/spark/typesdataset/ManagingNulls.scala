package org.github.explore.spark.typesdataset

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col}

object ManagingNulls {

  val spark = SparkSession.builder()
    .appName("ManagingNulls")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  def getOneOrAnotherColumnIfNull(): sql.DataFrame = {
    moviesDF.select(
      col("Title"),
      col("Rotten_Tomatoes_Rating"),
      col("IMDB_Rating"),
      coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10).as("Rotten_or_IMDB")
    )
  }

  def removingNulls(): sql.DataFrame = {
    moviesDF
      .select("Title", "IMDB_Rating", "Rotten_Tomatoes_Rating")
      .na.drop() // remove rows containing nulls
  }

  def replaceNulls(): sql.DataFrame = {
    // replace nulls
    // moviesDF.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating"))
    moviesDF.na.fill(Map(
      "IMDB_Rating" -> 0,
      "Rotten_Tomatoes_Rating" -> 10,
      "Director" -> "Unknown"
    ))
  }

  def complexOperationsWithNull(): sql.DataFrame = {
    moviesDF.selectExpr(
      "Title",
      "IMDB_Rating",
      "Rotten_Tomatoes_Rating",
      "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as ifnull", // same as coalesce
      "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl", // same
      "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nullif", // returns null if the two values are EQUAL, else first value
      "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) as nvl2" // if (first != null) second else third
    )
  }
}
