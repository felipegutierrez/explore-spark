package org.github.explore.spark.dataframes

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object DataFramesBasics {
    def main(args: Array[String]): Unit = {
      run()
    }

  def run() = {

    // creating a Spark Session
    val sparkSession = SparkSession.builder()
      .appName("DataFramesBasics")
      .config("spark.master", "local")
      .getOrCreate()

    // reading a Data Frame
    val firstDF: sql.DataFrame = sparkSession.read
      .format("json")
      .option("inferSchema", "true")
      .load("src/main/resources/data/cars.json")
    // showing a data frame and schema
    firstDF.show()
    firstDF.printSchema()

    // get rows
    firstDF.take(10).foreach(println)

    // spark types
    val longType = LongType

    // defining a schema
    val carsSchema = StructType(Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Year", StringType),
      StructField("Origin", StringType)
    ))

    val carsDFSchema = firstDF.schema
    println(carsDFSchema)

    println("read a DataFrame with my onw schema...")
    val carsDFWithSchema = sparkSession.read
      .format("json")
      .schema(carsSchema)
      .load("src/main/resources/data/cars.json")
    carsDFWithSchema.show()

    println("creating DataFrames from tuples")
    val cars = Seq(
      ("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA"),
      ("buick skylark 320", 15.0, 8L, 350.0, 165L, 3693L, 11.5, "1970-01-01", "USA"),
      ("plymouth satellite", 18.0, 8L, 318.0, 150L, 3436L, 11.0, "1970-01-01", "USA")
    )
    val manualCarsDF = sparkSession.createDataFrame(cars) // schema auto-inferred

    // create DFs with implicipts
    // import spark.implicits._
    // val manualCarsDFWithImplicits = cars.toDF("Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "CountryOrigin")
    // manualCarsDFWithImplicits.printSchema()

    /**
     * Exercise:
     * 1) Create a manual DF describing smartphones
     *   - make
     *   - model
     *   - screen dimension
     *   - camera megapixels
     */
    val smartphones = Seq(
      ("LG", "K40", "20x30", 12.0, 16000, 2),
      ("LG", "K80", "21x32", 12.0, 32000, 4),
      ("Samsumg", "Moto", "20x30", 12.0, 16000, 2),
    )
    val smartphonesDF = sparkSession.createDataFrame(smartphones).toDF("make", "model", "screen_dimension", "camera_megapixels", "memory", "cores")
    println(s"smartphonesDF: ${smartphonesDF.show()}")

    /** 2) Read another file from the data/ folder, e.g. movies.json
     *   - print its schema
     *   - count the number of rows, call count()
     */

    println("read movies.json from data folder onw schema...")
    // {"Title":"The Land Girls","US_Gross":146083,"Worldwide_Gross":146083,"US_DVD_Sales":null,
    // "Production_Budget":8000000,"Release_Date":"12-Jun-98","MPAA_Rating":"R","Running_Time_min":null,
    // "Distributor":"Gramercy","Source":null,"Major_Genre":null,"Creative_Type":null,"Director":null,
    // "Rotten_Tomatoes_Rating":null,"IMDB_Rating":6.1,"IMDB_Votes":1071}
    val moviesSchema = StructType(Array(
      StructField("Title", StringType),
      StructField("US_Gross", LongType),
      StructField("Worldwide_Gross", LongType),
      StructField("US_DVD_Sales", LongType),
      StructField("Production_Budget", LongType),
      StructField("Release_Date", StringType),
      StructField("MPAA_Rating", StringType),
      StructField("Running_Time_min", StringType),
      StructField("Distributor", StringType),
      StructField("Source", StringType),
      StructField("Major_Genre", StringType),
      StructField("Creative_Type", StringType),
      StructField("Director", StringType),
      StructField("Rotten_Tomatoes_Rating", StringType),
      StructField("IMDB_Rating", DoubleType),
      StructField("IMDB_Votes", LongType),
    ))
    val moviesDFWithSchema = sparkSession.read
      .format("json")
      .schema(moviesSchema) // .option("inferSchema", "true")
      .load("src/main/resources/data/movies.json")
    moviesDFWithSchema.show()
    println(s"counting movies: ${moviesDFWithSchema.count()}")
    moviesDFWithSchema.printSchema()
    moviesDFWithSchema.take(10).foreach(println)

    // difference between 2 DFs
    val carsTwo = Seq(
      ("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA"),
      ("buick skylark 320", 15.0, 8L, 350.0, 165L, 3693L, 11.5, "1970-01-01", "USA"),
      ("plymouth satellite", 18.0, 8L, 318.0, 150L, 3436L, 11.0, "1970-01-01", "USA"),
      ("amc rebel sst", 16.0, 8L, 304.0, 150L, 3433L, 12.0, "1970-01-01", "USA")
    )
    val manualCarsTwoDF = sparkSession.createDataFrame(carsTwo)
    println(s"resultDF union -> distinct:")
    manualCarsDF.union(manualCarsTwoDF).distinct().show()
    println(s"resultDF intersect: ")
    manualCarsDF.intersect(manualCarsTwoDF).show()
    println(s"resultDF except: ")
    manualCarsTwoDF.except(manualCarsDF).show()
  }
}
