package org.github.explore.spark.dataframes

import org.apache.spark.sql.AnalysisException
import org.github.explore.spark.SharedSparkSession
import org.scalatest.funsuite.AnyFunSuite

class DataFrameJoinsSpec extends AnyFunSuite with SharedSparkSession {

  test("an inner join guitar players with bands should result in a 3 rows data frame") {
    val guitaristsBandDF = DataFrameJoins.runJoin(DataFrameJoins.MyJoinType.INNER_JOIN)
    val count = guitaristsBandDF.count()
    assertResult(3L)(count)
  }
  test("an left outer join") {
    val allGuitaristsBandDF = DataFrameJoins.runJoin(DataFrameJoins.MyJoinType.LEFT_OUTER)
    val count = allGuitaristsBandDF.count()
    assertResult(4L)(count)
  }
  test("an right outer join") {
    val guitaristsAllBandDF = DataFrameJoins.runJoin(DataFrameJoins.MyJoinType.RIGHT_OUTER)
    val count = guitaristsAllBandDF.count()
    assertResult(4L)(count)
  }
  test("an outer join") {
    val allGuitaristsAllBandDF = DataFrameJoins.runJoin(DataFrameJoins.MyJoinType.OUTER)
    val count = allGuitaristsAllBandDF.count()
    assertResult(5L)(count)
  }
  test("a left semi join should return only columns from the left data frame") {
    val leftSemiJoin = DataFrameJoins.runJoin(DataFrameJoins.MyJoinType.LEFT_SEMI)
    val count = leftSemiJoin.count()
    leftSemiJoin.show
    leftSemiJoin.printSchema()
    val expectedColumns = Array("band", "guitars", "id", "name")
    val columns = leftSemiJoin.columns
    assertResult(3L)(count)
    assertResult(expectedColumns)(columns)
  }
  test("a left anti join should return only rows of the right table that do not satisfy the left join condition") {
    val leftAntiJoin = DataFrameJoins.runJoin(DataFrameJoins.MyJoinType.LEFT_ANTI)
    val count = leftAntiJoin.count()
    leftAntiJoin.show
    leftAntiJoin.printSchema()
    val expectedColumns = Array("band", "guitars", "id", "name")
    val columns = leftAntiJoin.columns
    assertResult(1L)(count)
    assertResult(expectedColumns)(columns)
  }
  test("selecting ambiguous columns should throw a runtime exception") {
    assertThrows[AnalysisException] {
      val runtimeException = DataFrameJoins.selectWithoutRenaming()
      runtimeException.show()
    }
  }
  test("selecting with drop duplicated column should not throw a runtime exception") {
    val runtimeException = DataFrameJoins.selectWithDropingDuplicatedColumns()
    runtimeException.printSchema()
    runtimeException.show()
    val expectedColumns = Array("band", "guitars", "id", "name", "hometown", "band_name", "year")
    val columns = runtimeException.columns
    assertResult(expectedColumns)(columns)
  }
  test("a join with complex expression") {
    val df = DataFrameJoins.runJoinWithComplexExpression(DataFrameJoins.MyJoinType.INNER_JOIN)
    df.printSchema()
    df.show()
    val count = df.count()
    assertResult(5L)(count)
  }
}
