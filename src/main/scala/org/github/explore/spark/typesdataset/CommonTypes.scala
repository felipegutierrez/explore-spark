package org.github.explore.spark.typesdataset

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.github.explore.spark.dataframes.DataFrameJoins

object CommonTypes {
  val spark = SparkSession.builder()
    .appName(DataFrameJoins.getClass.getSimpleName)
    .config("spark.master", "local")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    run()
  }

  def run() = {
    addPlainValueColumn("plain_value").show()
    evaluateBooleansAsColumns().show()
    onlyGoodMovies().show()
    getCorrelationOf("Rotten_Tomatoes_Rating", "IMDB_Rating")
  }

  def addPlainValueColumn(newColumn: String): sql.DataFrame = {
    val moviesDF = getDataFrameFromJson("src/main/resources/data/movies.json")
    moviesDF.select(col("Title"), lit(47).as(newColumn))
  }

  def onlyGoodMovies(): sql.DataFrame = {
    // it is the same of good_movie === true
    evaluateBooleansAsColumns().where("good_movie")
  }

  def evaluateBooleansAsColumns(): sql.DataFrame = {
    val dramaFilter = col("Major_Genre") equalTo "Drama"
    val goodRatingFilter = col("IMDB_Rating") > 7.0
    val preferredFilter = dramaFilter and goodRatingFilter
    val moviesDF = getDataFrameFromJson("src/main/resources/data/movies.json")
      .select(col("Title"), preferredFilter.as("good_movie"))
    moviesDF
  }

  def getCorrelationOf(column1: String, column2: String): Double = {
    val moviesDF = getDataFrameFromJson("src/main/resources/data/movies.json")

    /**
     * Calculates the Pearson Correlation Coefficient of two columns of a DataFrame.
     * Params:
     * col1 – the name of the column
     * col2 – the name of the column to calculate the correlation against
     * Returns:
     * The Pearson Correlation Coefficient as a Double.
     */
    moviesDF.stat.corr(column1, column2)
  }

  def onlyBadMovies(): sql.DataFrame = {
    // it is the same of good_movie =!= true or using not as negation
    evaluateBooleansAsColumns().where(not(col("good_movie")))
  }

  def getCorrelationOf(): sql.DataFrame = {
    val moviesDF = getDataFrameFromJson("src/main/resources/data/movies.json")
    // moviesDF.select(initcap(col("Title")))
    moviesDF.select(upper(col("Title")).as("upper_title"))
  }

  def getDataFrameFromJson(pathJson: String): sql.DataFrame = {
    spark.read
      .option("inferSchema", "true")
      .json(pathJson)
  }

  def usingRegexOnDataframes(regex: String): sql.DataFrame = {
    val carsDF = getDataFrameFromJson("src/main/resources/data/cars.json")
    carsDF
      .select(col("Name"), regexp_extract(col("Name"), regex, 0).as("regex_extract"))
      .where(col("regex_extract") =!= "")
    // .drop("regex_extract")
  }

  def usingRegexToReplaceOnDataframes(regex: String): sql.DataFrame = {
    val carsDF = getDataFrameFromJson("src/main/resources/data/cars.json")
    carsDF
      .select(col("Name"), regexp_extract(col("Name"), regex, 0).as("regex_extract"))
      .where(col("regex_extract") =!= "")
      .drop("regex_extract")
      .select(col("Name"), regexp_replace(col("Name"), regex, "People's car").as("regex_replace"))
  }

  /**
   * Exercise
   *
   * Filter the cars DF by a list of car names obtained by an API call
   * Versions:
   *   - contains
   *   - regexes
   */
  def getCarNames: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")

  def getCarDfWithRegex(filter: List[String]): sql.DataFrame = {
    val regex = filter.mkString("|").toLowerCase
    val carsDF = getDataFrameFromJson("src/main/resources/data/cars.json")
    carsDF
      .select(col("Name"), regexp_extract(col("Name"), regex, 0).as("regex_extract"))
      .where(col("regex_extract") =!= "")
    // .drop("regex_extract")
  }

  def getCarDfWithContain(filter: List[String]): sql.DataFrame = {
    val carsDF = getDataFrameFromJson("src/main/resources/data/cars.json")
    carsDF
      .select(col("*"))
      .where(
        filter
          .map(v => v.toLowerCase)
          .map(v => col("Name").contains(v))
          .reduce((a, b) => a || b)
      )
  }
}
