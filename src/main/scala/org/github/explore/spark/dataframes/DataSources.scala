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

  // Reading from a remote DB
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def main(args: Array[String]): Unit = {
    run()
  }

  def run() = {
    val dfFromJson = readDataFrameFromJson("src/main/resources/data/cars.json", carsSchema)
    dfFromJson.printSchema()
    dfFromJson.show()

    writeDataFrameFromJson(dfFromJson, "target/output/data/cars_dupe.json")

    val dfFromJson2 = readDataFrameFromJson("target/output/data/cars_dupe.json", carsSchema)
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
      ("dateFormat" -> "MMM d yyyy"),
      ("header" -> "true"),
      ("sep" -> ","),
      ("nullValue" -> "")
    )
    val csvDF = readCsvFileWithOptions("src/main/resources/data/stocks.csv", opts)
    csvDF.show()

    saveParquetDataFrame(dfFromJson, "target/output/data/parquet/cars.parquet")

    readFromPostgreSql()

    /**
     * Exercise: read the movies DF, then write it as
     * - tab-separated values file
     * - snappy Parquet
     * - table "public.movies" in the Postgres DB
     */
    val moviesDF = readDataFrameFromJson("src/main/resources/data/movies.json")
    moviesDF.printSchema()
    moviesDF.show()

    val moviesOptions = Map(
      ("dateFormat" -> "MMM d yyyy"),
      ("header" -> "true"),
      ("sep" -> "\t"),
      ("nullValue" -> "")
    )
    writeDataFrameAsCsvFileWithOptions(moviesDF, moviesOptions, "target/output/data/csv/movies.csv")
    saveParquetDataFrame(moviesDF, "target/output/data/parquet/movies.parquet")

    val moviesJdbcOptions = Map(
      ("driver" -> driver),
      ("url" -> url),
      ("user" -> user),
      ("password" -> password),
      ("dbtable" -> "public.movies"))
    saveDataFrameOnPostgreSql(moviesDF, moviesJdbcOptions)
  }

  def readDataFrameFromJson(jsonFile: String, structType: StructType) = {
    /**
     * reading a Data Frame options
     * permissive (default) = Sparks tries its best to load the whole dataframe
     * failFast = fails the DF before the load is executed if there is a malformed format
     * dropMalformed =
     */
    val carsDF: sql.DataFrame = sparkSession.read
      .format("json")
      .schema(structType) // .option("inferSchema", "true")
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

  def readDataFrameFromJson(jsonFile: String) = {
    val dataFrame: sql.DataFrame = sparkSession.read
      .json(jsonFile)
    dataFrame
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
    println(s"reading from CSV file: $csvPath")
    val csvDF = sparkSession.read
      .schema(stocksSchema)
      .options(opts)
      .csv(csvPath)
    csvDF
  }

  def writeDataFrameAsCsvFileWithOptions(dataFrame: sql.DataFrame, opts: Map[String, String], targetCsvFile: String) = {
    println(s"writing data frame in CSV file $targetCsvFile")
    dataFrame.write
      .format("csv")
      .options(opts)
      .mode(SaveMode.Overwrite)
      .save(targetCsvFile)
  }

  def saveParquetDataFrame(dataFrame: sql.DataFrame, path: String) = {
    // Parquet
    dataFrame.write
      .mode(SaveMode.Overwrite)
      .save(path)
  }

  def readFromPostgreSql(postgreSqlTable: String = "public.employees"): sql.DataFrame = {
    val dataframe = sparkSession.read
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", postgreSqlTable)
      .load()
    dataframe
  }

  def saveDataFrameOnPostgreSql(dataFrame: sql.DataFrame, opts: Map[String, String]) = {
    println(s"saving data frame ${dataFrame.printSchema()} into postgreSql table: ${opts.foreach(println)}")
    dataFrame.write
      .format("jdbc")
      .options(opts)
      .save()
  }
}
