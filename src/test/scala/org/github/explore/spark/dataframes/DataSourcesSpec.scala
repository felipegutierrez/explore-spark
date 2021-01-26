package org.github.explore.spark.dataframes

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.funsuite.AnyFunSuite

class DataSourcesSpec extends AnyFunSuite with SharedSparkContext {

  test("read and save a data frame as another JSON files must be equal files") {

    val dfFromJson1 = DataSources.readDataFrameFromJson("src/main/resources/data/cars.json")
    val df01Count = dfFromJson1.count()

    DataSources.writeDataFrameFromJson(dfFromJson1, "target/output/data/cars_dupe.json")
    val dfFromJson2 = DataSources.readDataFrameFromJson("target/output/data/cars_dupe.json")

    val count = dfFromJson1.toJavaRDD.union(dfFromJson2.toJavaRDD).count()

    assertResult((df01Count * 2))(count)
  }
  test("read and save a data frame as another JSON files must be equal files when using distinct to count") {

    val dfFromJson1 = DataSources.readDataFrameFromJson("src/main/resources/data/cars.json")
    val df01Count = dfFromJson1.count()

    DataSources.writeDataFrameFromJson(dfFromJson1, "target/output/data/cars_dupe.json")
    val dfFromJson2 = DataSources.readDataFrameFromJson("target/output/data/cars_dupe.json")

    val countDistinct = dfFromJson1.toJavaRDD.union(dfFromJson2.toJavaRDD).distinct().count()

    assertResult(df01Count)(countDistinct)
  }
}
