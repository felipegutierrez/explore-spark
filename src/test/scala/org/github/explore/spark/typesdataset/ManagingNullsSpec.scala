package org.github.explore.spark.typesdataset

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite

class ManagingNullsSpec extends AnyFunSuite with SharedSparkContext {
  test("using coalesce must return one or another column") {
    val dataFrameWithCoalesce = ManagingNulls.getOneOrAnotherColumnIfNull()
    dataFrameWithCoalesce.show
    assertResult(Array[String]("Title", "Rotten_Tomatoes_Rating", "IMDB_Rating", "Rotten_or_IMDB"))(dataFrameWithCoalesce.columns)
  }
  test("not available with drop should remove nulls") {
    val dataFrameWithoutNulls = ManagingNulls.removingNulls()
    dataFrameWithoutNulls.show
    val count = dataFrameWithoutNulls
      .where(
        col("Rotten_Tomatoes_Rating").isNull ||
          col("IMDB_Rating").isNull
      ).count
    assertResult(0)(count)
  }
  test("replacing nulls should result in columns with value") {
    val dataFrameWithReplacedNUlls = ManagingNulls.replaceNulls()
    dataFrameWithReplacedNUlls.show
    val count = dataFrameWithReplacedNUlls
      .where(col("Director") === "Unknown")
      .count
    assert(count > 0)
  }
  test("replacing nulls with query declarative language") {
    val dataFrame = ManagingNulls.complexOperationsWithNull()
    dataFrame.show
  }
}
