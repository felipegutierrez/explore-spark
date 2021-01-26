package org.github.explore.spark.dataframes

import org.apache.spark.sql
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataSources {
//  def main(args: Array[String]): Unit = {
//    run()
//  }

  def run() = {
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
      StructField("Year", StringType),
      StructField("Origin", StringType)
    ))

    /**
     * reading a Data Frame options
     * permissive (default) = Sparks tries its best to load the whole dataframe
     * failFast = fails the DF before the load is executed if there is a malformed format
     * dropMalformed =
     */
    val carsDF: sql.DataFrame = sparkSession.read
      .format("json")
      .schema(carsSchema) // .option("inferSchema", "true")
      .option("mode", "failFast")
      .option("path", "src/main/resources/data/cars.json")
      .load()
    // showing a data frame and schema
    // carsDF.show()
    // carsDF.printSchema()

    /*
   Writing DFs
   - format
   - save mode = overwrite, append, ignore, errorIfExists
   - path
   - zero or more options
  */
    println("Writing DFs")
    carsDF.write
      .format("json")
      .mode(SaveMode.Overwrite)
      .save("target/output/data/cars_dupe.json")

    // alternative reading with options map
    val carsDFWithOptionMap = sparkSession.read
      .format("json")
      .options(Map(
        "mode" -> "failFast",
        "path" -> "target/output/data/cars_dupe.json",
        "inferSchema" -> "true"
      ))
      .load()

  }
}
