package org.github.explore.spark.rdd

import org.apache.spark.rdd.RDD
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

  def readStocks(fileName: String): List[StockValue] =
    Source.fromFile(fileName)
      .getLines
      .drop(1)
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList
}
