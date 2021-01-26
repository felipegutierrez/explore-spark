package org.github.explore.spark.dataframes

import org.apache.spark.sql
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataSources {
  val sparkSession = SparkSession.builder()
    .appName("DataSources")
    .config("spark.master", "local")
    .getOrCreate()

  // defining a schema
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

  // CSV flags
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  //  def main(args: Array[String]): Unit = {
  //    run()
  //  }

  def run() = {
    val dfFromJson = readDataFrameFromJson("src/main/resources/data/cars.json")
    dfFromJson.printSchema()
    dfFromJson.show()

    writeDataFrameFromJson(dfFromJson, "target/output/data/cars_dupe.json")

    val dfFromJson2 = readDataFrameFromJson("target/output/data/cars_dupe.json")
    dfFromJson2.printSchema()
    dfFromJson2.show()


    // alternative reading with options map
    val carsDFWithOptionMap = sparkSession.read
      .format("json")
      .options(Map(
        "mode" -> "failFast",
        "path" -> "target/output/data/cars_dupe.json",
        "inferSchema" -> "true"
      ))
      .load()

    val opts = Map(
      ("dateFormat" -> "MMM dd YYYY"),
      ("header" -> "true"),
      ("sep" -> ","),
      ("nullValue" -> "")
    )
    val csvDF = readCsvFileWithOptions("src/main/resources/data/stocks.csv", opts)
    csvDF.show()

    saveParquetDataFrame(dfFromJson, "target/output/data/parquet/cars.parquet")
  }

  def readDataFrameFromJson(jsonFile: String) = {
    /**
     * reading a Data Frame options
     * permissive (default) = Sparks tries its best to load the whole dataframe
     * failFast = fails the DF before the load is executed if there is a malformed format
     * dropMalformed =
     */
    val carsDF: sql.DataFrame = sparkSession.read
      .format("json")
      .schema(carsSchema) // .option("inferSchema", "true")
      .option("dateFormat", "YYYY-MM-dd") // couple with schema; if Spark fails parsing, it will put null
      .option("allowSingleQuotes", "true")
      .option("mode", "failFast")
      .option("path", jsonFile)
      .load()
    // showing a data frame and schema
    // carsDF.show()
    // carsDF.printSchema()
    carsDF
  }

  def writeDataFrameFromJson(dataFrame: sql.DataFrame, targetJsonFile: String) = {
    /*
       Writing DFs
       - format
       - save mode = overwrite, append, ignore, errorIfExists
       - path
       - zero or more options
      */
    println("Writing DFs")
    dataFrame.write
      .format("json")
      .mode(SaveMode.Overwrite)
      .save(targetJsonFile)
  }

  def readCsvFileWithOptions(csvPath: String = "src/main/resources/data/stocks.csv", opts: Map[String, String]): sql.DataFrame = {
    // CSV
    val csvDF = sparkSession.read
      .schema(stocksSchema)
      .options(opts)
      .csv(csvPath)
    csvDF
  }

  def saveParquetDataFrame(dataFrame: sql.DataFrame, path: String) = {
    // Parquet
    dataFrame.write
      .mode(SaveMode.Overwrite)
      .save(path)
  }
}
