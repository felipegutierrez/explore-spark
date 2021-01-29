package org.github.explore.spark.dataframes

import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql.{Column, SparkSession}

object ColumnsAndExpressions {
  val spark = SparkSession.builder()
    .appName(ColumnsAndExpressions.getClass.getSimpleName)
    .config("spark.master", "local")
    .getOrCreate()

  //  def main(args: Array[String]): Unit = {
  //    run()
  //  }

  def run() = {
    val carsDF: sql.DataFrame = getDataFrameFromJson("src/main/resources/data/cars.json")

    // columns
    val firstColumn: Column = carsDF.col("Name")

    // selection (projection)
    val carsNameDf = carsDF.select(firstColumn)
    carsNameDf.printSchema()
    carsNameDf.show()

    // other ways to use selection
    import spark.implicits._
    val otherWaysOfSelection = carsDF.select(
      carsDF.col("Name"),
      col("Acceleration"),
      column("Weight_in_lbs"),
      'Year, // Scala symbol auto-converted to column (import spark.implicits._)
      $"HorsePower",
      expr("Origin")
    )
    otherWaysOfSelection.printSchema()
    otherWaysOfSelection.show()

    val simpleWaysOfSelection = carsDF.select("Name", "Year")
    simpleWaysOfSelection.printSchema()
    simpleWaysOfSelection.show()

    // Expressions
    val expressionLbs = carsDF.col("Weight_in_lbs")
    val expressionKg = carsDF.col("Weight_in_lbs") / 2.2
    val carsDfWithKg = carsDF.select(
      col("Name"),
      col("Weight_in_lbs"),
      expressionKg.as("Weight_in_kg"),
      expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
    )
    carsDfWithKg.printSchema()
    carsDfWithKg.show()

    val carsDfWithSelectExpression = carsDF.selectExpr(
      "Name", "Acceleration", "Weight_in_lbs", "Weight_in_lbs / 2.2"
    )
    carsDfWithSelectExpression.printSchema()
    carsDfWithSelectExpression.show()

    // adding a column
    val carsDFWithNewColumn = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
    carsDFWithNewColumn.printSchema()
    carsDFWithNewColumn.show()

    // renaming a column
    val newCarsDfWithNewColumn = carsDFWithNewColumn.withColumnRenamed("Weight_in_kg_3", "Weight in kg")
    newCarsDfWithNewColumn.printSchema()
    newCarsDfWithNewColumn.show()

    newCarsDfWithNewColumn.selectExpr("`Weight in kg`")
      .show()
    newCarsDfWithNewColumn.drop("Cylinders", "Displacement")
      .show()

    // filtering
    val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
    val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")
    europeanCarsDF.show()
    val americanCarsDF = carsDF.filter("Origin = 'USA'")
    americanCarsDF.show()
    // chain filters
    val americanPowerfulCarsDF = carsDF
      .filter(col("Origin") === "USA")
      .filter(col("Horsepower") > 150)
    americanPowerfulCarsDF.show()
    val americanPowerfulCarsDF2 = carsDF
      .filter(col("Origin") === "USA" and col("Horsepower") > 150)
    americanPowerfulCarsDF2.count()
    val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")
    americanPowerfulCarsDF3.count()

    // union operations
    val moreCarsDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/more_cars.json")
    val allCarsDF = carsDF.union(moreCarsDF)
    allCarsDF.count()

    val allCountries = getDataFrameWithDistinct(carsDF, "Origin")
    allCountries.show()
  }

  def getDataFrameWithDistinct(dataFrame: sql.DataFrame, column: String): sql.DataFrame = {
    dataFrame.select(column).distinct()
  }

  def getDataFrameFromJson(pathJson: String): sql.DataFrame = {
    spark.read
      .option("inferSchema", "true")
      .json(pathJson)
  }
}
