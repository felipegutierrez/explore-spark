package org.github.explore.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.Source
import scala.util.Try

object BasicsRDD {
  val spark = SparkSession.builder()
    .appName("BasicsRDD")
    .config("spark.master", "local")
    .getOrCreate()

  // the SparkContext is the entry point for low-level APIs, including RDDs
  val sc = spark.sparkContext

  def getCollectionParallelizedAndSum(begin: Long, end: Long = 1000000): Try[Double] = {
    val numbers = begin to end
    val numbersRDD = sc.parallelize(numbers)
    val sum = numbersRDD.sum
    Try(sum)
  }

  // 2 - reading from files
  def readRDDFromFile(fileName: String = "src/main/resources/data/stocks.csv"): Try[RDD[StockValue]] = Try {
    val stocksRDD = sc.parallelize(readStocks(fileName).getOrElse(List[StockValue]()))
    stocksRDD
  }

  def readStocks(filename: String): Try[List[StockValue]] = Try {
    Source.fromFile(filename)
      .getLines
      .drop(1)
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList
  }

  def readFromDataFrame(fileName: String = "src/main/resources/data/stocks.csv"): Try[RDD[StockValue]] = Try {
    val stocksDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(fileName)
    import spark.implicits._
    val stocksDS = stocksDF.as[StockValue]
    val stocksRDD3 = stocksDS.rdd
    stocksRDD3
  }
}
