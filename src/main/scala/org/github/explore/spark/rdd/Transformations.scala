package org.github.explore.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source
import scala.util.Try

object Transformations {

  val spark = SparkSession.builder()
    .appName("BasicsRDD")
    .config("spark.master", "local")
    .getOrCreate()

  // the SparkContext is the entry point for low-level APIs, including RDDs
  val sc = spark.sparkContext

  def filtering(fileName: String = "src/main/resources/data/stocks.csv", filter: String = "MSFT"): Try[Long] = Try {
    val stocksRDD = readRDDFromFile(fileName)
    val count = stocksRDD
      .filter(_.symbol == filter)
      .count
    count
  }

  def readRDDFromFile(fileName: String = "src/main/resources/data/stocks.csv"): RDD[StockValue] = {
    val stocksRDD = sc.parallelize(readStocks(fileName))
    stocksRDD
  }

  def readStocks(fileName: String): List[StockValue] =
    Source.fromFile(fileName)
      .getLines
      .drop(1)
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

  def getComparison() = {
    // min and max
    implicit val stockOrdering: Ordering[StockValue] =
      Ordering.fromLessThan[StockValue]((sa: StockValue, sb: StockValue) => sa.price < sb.price)
    val minMsft = readRDDFromFile().min() // action
    minMsft
  }

  def getRepartition() = {
    // Partitioning
    import spark.implicits._
    val repartitionedStocksRDD = readRDDFromFile().repartition(30)
    repartitionedStocksRDD.toDF.write
      .mode(SaveMode.Overwrite)
      .parquet("src/main/resources/data/stocks30")
  }

  /** 2. Show the distinct genres as an RDD. */
  def distinctGenre() = {
    readMoviesRDDFromFile()
      .map(_.genre)
      .distinct
  }

  /**
   * Exercises
   *
   * 1. Read the movies.json as an RDD.
   * 2. Show the distinct genres as an RDD.
   * 3. Select all the movies in the Drama genre with IMDB rating > 6.
   * 4. Show the average rating of movies by genre.
   */

  def readMoviesRDDFromFile(fileName: String = "src/main/resources/data/movies.json"): RDD[Movie] = {
    val moviesDF = spark
      .read
      .option("inferSchema", "true")
      .json(fileName)
    import spark.implicits._
    val moviesRDD = moviesDF
      .select(col("Title").as("title"), col("Major_Genre").as("genre"), col("IMDB_Rating").as("rating"))
      .where(col("genre").isNotNull and col("rating").isNotNull)
      .as[Movie]
      .rdd
    moviesRDD
  }

  /** 3. Select all the movies in the Drama genre with IMDB rating > 6. */
  def getAllMoviesWithRatingGreaterThan(rating: Double) = {
    readMoviesRDDFromFile()
      .filter(movie => movie.genre == "Drama" && movie.rating > rating)
  }

  /** 4. Show the average rating of movies by genre */
  def getAvgByGenre() = {
    readMoviesRDDFromFile()
      .groupBy(_.genre)
      .map { case (genre, movies) =>
        GenreAvgRating(genre, movies.map(_.rating).sum / movies.size)
      }
  }

  case class Movie(title: String, genre: String, rating: Double)

  case class GenreAvgRating(genre: String, rating: Double)

}
