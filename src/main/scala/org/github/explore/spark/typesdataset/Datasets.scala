package org.github.explore.spark.typesdataset

import org.apache.spark.sql.functions.{array_contains, col}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

import java.sql.Date

object Datasets {

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF: DataFrame = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/numbers.csv")
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  def convertDataFrameToDataSet(): Dataset[Int] = {
    // convert a DF to a Dataset
    implicit val intEncoder = Encoders.scalaInt
    val numbersDS: Dataset[Int] = numbersDF.as[Int]
    numbersDS
  }

  def averageOfHorsePowerInCarsDataset(): Double = {
    val dataSetOfCars = convertComplexDataFrameToDataSet()
    import Datasets.spark.implicits._
    val sum = dataSetOfCars
      .map(car => car.Horsepower.getOrElse[Long](0))
      .reduce(_ + _)
    val count = dataSetOfCars.count
    val average: Double = sum / count
    println(s" sum: $sum  / count: $count = $average")
    average
  }

  def joinGuitarPlayerWithBand(): Dataset[(GuitarPlayer, Band)] = {
    import spark.implicits._
    // val guitarsDS = readDF("guitars.json").as[Guitar]
    val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
    val bandsDS = readDF("bands.json").as[Band]

    val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayersDS
      .joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"), "inner")
    guitarPlayerBandsDS
  }

  def joinGuitarWithGuitarPlayer() = {
    import spark.implicits._
    val guitarsDS = readDF("guitars.json").as[Guitar]
    val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]

    val guitarWithGuitarPlayersDS = guitarsDS
      .joinWith(guitarPlayersDS, array_contains(col("guitars"), guitarsDS.col("id")), "outer")

    guitarWithGuitarPlayersDS
  }

  def readDF(filename: String) = spark
    .read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")

  def groupingCarsByOrigin() = {
    val carsDS = convertComplexDataFrameToDataSet()
    import spark.implicits._
    carsDS
      .groupByKey(_.Origin)
      .count
  }

  def convertComplexDataFrameToDataSet() = {
    // dataset of a complex type

    // 2 - read the DF from the file
    val carsDF = readDFWithSchema("cars.json", carsSchema)

    // 3 - define an encoder (importing the implicits)
    import spark.implicits._
    // implicit val carEncoder = Encoders.product[Car]
    // 4 - convert the DF to DS
    val carsDS = carsDF.as[Car]
    carsDS
  }

  def readDFWithSchema(filename: String, schema: StructType) = spark
    .read
    .schema(schema)
    .json(s"src/main/resources/data/$filename")

  // 1 - define your case class
  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: Date,
                  Origin: String
                )

  // Joins
  case class Guitar(id: Long, make: String, model: String, guitarType: String)

  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)

  case class Band(id: Long, name: String, hometown: String, year: Long)

}
