package org.github.explore.spark.dataframes

import org.apache.spark.sql
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConverters._

object DataFramesBasicUnions {
  // creating a Spark Session
  val sparkSession = SparkSession.builder()
    .appName("DataFramesBasics")
    .config("spark.master", "local")
    .getOrCreate()
  val spark2 = sparkSession

  val milesMapType: MapType = DataTypes.createMapType(StringType, DoubleType)
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles", milesMapType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  val carsOne: java.util.List[Row] = List(
    Row("chevrolet chevelle malibu", Map("gallon" -> 18.0), 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA"),
    Row("buick skylark 320", Map("gallon" -> 15.0, "liters" -> 13.0), 8L, 350.0, 165L, 3693L, 11.5, "1970-01-01", "USA"),
    Row("plymouth satellite", Map("gallon" -> 18.0), 8L, 318.0, 150L, 3436L, 11.0, "1970-01-01", "USA")
  ).asJava

  val carsTwo: java.util.List[Row] = List(
    Row("chevrolet chevelle malibu", Map("gallon" -> 18.0, "liters" -> 15.0), 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA"),
    Row("buick skylark 320", Map("gallon" -> 15.0), 8L, 350.0, 165L, 3693L, 11.5, "1970-01-01", "USA"),
    Row("plymouth satellite", Map("gallon" -> 18.0), 8L, 318.0, 150L, 3436L, 11.0, "1970-01-01", "USA"),
    Row("amc rebel sst", Map("gallon" -> 16.0), 8L, 304.0, 150L, 3433L, 12.0, "1970-01-01", "USA")
  ).asJava

  def main(args: Array[String]): Unit = {
    run()
  }

  def run() = {
    val firstDF = createDataFrame(carsOne)
    val secondDF = createDataFrame(carsTwo)
    val thirdDF = unionDistinct(firstDF, secondDF)
    thirdDF.foreach(row => println(row))
    val fourthDF = intersect(firstDF, secondDF)
    fourthDF.foreach(row => println(row))
  }

  def createDataFrame(list: java.util.List[Row]): sql.DataFrame = {
    val manualCarsDFTwo = sparkSession.createDataFrame(list, carsSchema)
    manualCarsDFTwo.show()
    manualCarsDFTwo
  }

  def unionDistinct(df1: sql.DataFrame, df2: sql.DataFrame): sql.DataFrame = {
    val resultRDD = df1.toJavaRDD.union(df2.toJavaRDD).distinct()
    val resultDF = sparkSession.createDataFrame(resultRDD, carsSchema)
    resultDF
  }

  def intersect(df1: sql.DataFrame, df2: sql.DataFrame): sql.DataFrame = {
    println(s"resultDF intersect: ")
    val resultRDD = df1.toJavaRDD.intersection(df2.toJavaRDD)
    val resultDF = sparkSession.createDataFrame(resultRDD, carsSchema)
    resultDF
  }

  def union(df1: sql.DataFrame, df2: sql.DataFrame): sql.DataFrame = {
    val resultRDD = df1.toJavaRDD.union(df2.toJavaRDD)
    val resultDF = sparkSession.createDataFrame(resultRDD, carsSchema)
    resultDF
  }
}
