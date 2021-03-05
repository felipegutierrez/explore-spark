package org.github.explore.spark.dataframes

import org.github.explore.spark.SharedSparkSession
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class DataSourcesSpec extends AnyFunSuite with SharedSparkSession {

  // We can import sql implicits
  // import sqlImplicits._

  test("read and save a data frame as another JSON files must be equal files") {

    val dfFromJson1 = DataSources.readDataFrameFromJson("src/main/resources/data/cars.json", DataSources.carsSchema)
    val df01Count = dfFromJson1.count()

    DataSources.writeDataFrameFromJson(dfFromJson1, "target/output/data/cars_dupe.json")
    val dfFromJson2 = DataSources.readDataFrameFromJson("target/output/data/cars_dupe.json", DataSources.carsSchema)

    val count = dfFromJson1.toJavaRDD.union(dfFromJson2.toJavaRDD).count()

    assertResult((df01Count * 2))(count)
  }
  test("read and save a data frame as another JSON files must be equal files when using distinct to count") {

    val dfFromJson1 = DataSources.readDataFrameFromJson("src/main/resources/data/cars.json", DataSources.carsSchema)
    val df01Count = dfFromJson1.count()

    DataSources.writeDataFrameFromJson(dfFromJson1, "target/output/data/cars_dupe.json")
    val dfFromJson2 = DataSources.readDataFrameFromJson("target/output/data/cars_dupe.json", DataSources.carsSchema)

    val countDistinct = dfFromJson1.toJavaRDD.union(dfFromJson2.toJavaRDD).distinct().count()

    assertResult(df01Count)(countDistinct)
  }
  test("read CSV files correctly") {
    val options = Map(
      ("dateFormat" -> "MMM dd YYYY"),
      ("header" -> "true"),
      ("sep" -> ","),
      ("nullValue" -> "")
    )
    val csvDF = DataSources.readCsvFileWithOptions("src/main/resources/data/stocks.csv", options)
    assertResult(560)(csvDF.count())
  }
  test("size of Parquet binary files for Data Frames must be 6x smaller than Json") {
    val json: String = "src/main/resources/data/cars.json"
    val parquet: String = "target/output/data/parquet/cars.parquet"
    val dfFromJson1 = DataSources.readDataFrameFromJson(json, DataSources.carsSchema)
    DataSources.saveParquetDataFrame(dfFromJson1, parquet)

    val jsonSize = (new File(json)).length
    val parquetSize = (new File(parquet)).length
    val timesBigger: Double = jsonSize / parquetSize
    println(s"jsonSize: $jsonSize parquetSize: $parquetSize timesBigger: $timesBigger")
    assert(timesBigger > 6)
  }
}
