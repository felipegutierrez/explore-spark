package org.github.explore.spark.typesdataset

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Calendar

object ComplexTypes {

  val spark = SparkSession.builder()
    .appName(ComplexTypes.getClass.getSimpleName)
    .config("spark.master", "local")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    run()
  }

  def run() = {
    getMoviesWithReleaseDate().show()
  }

  def getMoviesWithReleaseDate(): sql.DataFrame = {
    val custom_to_date = udf(parseDate _)
    val moviesDF = getDataFrameFromJson("src/main/resources/data/movies.json")
    moviesDF
      // .select(col("Title"), to_date(col("Release_Date"), "d-MMM-yy").as("Actual_Release")) // it is parsing 98 to 2098, not 1998
      .select(
        col("Title"),
        to_date(custom_to_date(col("Release_Date"), lit("d-MMM-yy"))).as("Actual_Release")
      )
      .withColumn("Today", current_date())
      .withColumn("Right_Now", current_timestamp())
      .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365)
  }

  /**
   * using SimpleDateFormat to format years with 2 digits into 4 digits on the previous century.
   *
   * @param date
   * @param pattern
   * @return
   */
  def parseDate(date: String, pattern: String): Date = {
    val cal = Calendar.getInstance()
    cal.set(Calendar.YEAR, 1923)
    val beginning = cal.getTime

    // println(date)
    val dateArr = if (date != null) date.split("-") else Array("")
    if (dateArr.length == 3) {
      val newDay = dateArr(0)

      val month = dateArr(1)
      val newMonth = if (month.matches("[0-9]+")) {
        println(s"numbers not allowed on the month field: $month")
        if (month.toInt == 1) "Jan"
        else if (month.toInt == 2) "Feb"
        else if (month.toInt == 3) "Mar"
        else if (month.toInt == 4) "Apr"
        else if (month.toInt == 5) "May"
        else if (month.toInt == 6) "Jun"
        else if (month.toInt == 7) "Jul"
        else if (month.toInt == 8) "Aug"
        else if (month.toInt == 9) "Sep"
        else if (month.toInt == 10) "Oct"
        else if (month.toInt == 11) "Nov"
        else if (month.toInt == 12) "Dez"
        else "Jan"
      } else {
        month
      }

      val year = dateArr(2)
      val newYear =
        if (year.length == 4) year.substring(2, 4)
        else year

      val newDate = newDay + "-" + newMonth + "-" + newYear
      // println("newDate: " + newDate)

      val format = new SimpleDateFormat(pattern)
      format.set2DigitYearStart(beginning)
      new Date(format.parse(newDate).getTime)
    } else {
      new Date(System.currentTimeMillis)
    }
  }

  def getMoviesWithStructures(): sql.DataFrame = {
    val moviesDF = getDataFrameFromJson("src/main/resources/data/movies.json")
    moviesDF
      .select(
        col("Title"),
        struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit")
      )
      .select(
        col("Title"),
        col("Profit"),
        col("Profit").getField("US_Gross").as("US_Profit")
      )
  }

  def getDataFrameFromJson(pathJson: String): sql.DataFrame = {
    spark.read
      .option("inferSchema", "true")
      .json(pathJson)
  }

  def getMoviesWithStructuresUsingExpressions(): sql.DataFrame = {
    val moviesDF = getDataFrameFromJson("src/main/resources/data/movies.json")
    moviesDF
      .selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
      .selectExpr("Title", "Profit", "Profit.US_Gross as US_Profit")
  }

  def getMoviesWithStructuresUsingArrays(): sql.DataFrame = {
    val moviesDF = getDataFrameFromJson("src/main/resources/data/movies.json")
    val moviesWithWords = moviesDF
      .select(
        col("Title"),
        split(col("Title"), " |,").as("Title_Words") // creates an array
      )
    moviesWithWords
      .select(
        col("Title"),
        expr("Title_Words[0]"),
        size(col("Title_Words")),
        array_contains(col("Title_Words"), "Love").as("boolean")
      )
  }

  def getStocksWithReleaseDate(): sql.DataFrame = {
    val custom_to_date = udf(parseDate _)
    // symbol,date,price
    // MSFT,Jan 1 2000,39.81
    val stocksSchema = StructType(Array(
      StructField("symbol", StringType),
      StructField("date", DateType),
      StructField("price", DoubleType)
    ))
    val opts = Map(
      ("dateFormat" -> "MMM d yyyy"),
      ("header" -> "true"),
      ("sep" -> ","),
      ("nullValue" -> "")
    )

    val stocksDF = readCsvFileWithOptions("src/main/resources/data/stocks.csv", stocksSchema, opts)
    stocksDF
      .select(
        col("symbol"),
        to_date(col("date"), "MMM d yyyy").as("date_formatted"),
        col("price")
      )
  }

  def readCsvFileWithOptions(csvPath: String = "src/main/resources/data/stocks.csv", schema: StructType, opts: Map[String, String]): sql.DataFrame = {
    println(s"reading from CSV file: $csvPath")
    val csvDF = spark.read
      .schema(schema)
      .options(opts)
      .csv(csvPath)
    csvDF
  }
}
