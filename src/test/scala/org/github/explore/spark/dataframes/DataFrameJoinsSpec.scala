package org.github.explore.spark.dataframes

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.funsuite.AnyFunSuite

class DataFrameJoinsSpec extends AnyFunSuite with SharedSparkContext {

  import DataFrameJoins._

  test("an inner join guitar players with bands should result in a 3 rows data frame") {
    val guitaristsBandDF = runJoin(MyJoinType.INNER_JOIN)
    val count = guitaristsBandDF.count()
    assertResult(3L)(count)
  }
  test("an left outer join") {
    val allGuitaristsBandDF = runJoin(MyJoinType.LEFT_OUTER)
    val count = allGuitaristsBandDF.count()
    assertResult(4L)(count)
  }
  test("an right outer join") {
    val guitaristsAllBandDF = runJoin(MyJoinType.RIGHT_OUTER)
    val count = guitaristsAllBandDF.count()
    assertResult(4L)(count)
  }
  test("an outer join") {
    val allGuitaristsAllBandDF = runJoin(MyJoinType.OUTER)
    val count = allGuitaristsAllBandDF.count()
    assertResult(5L)(count)
  }
  test("a left semi join should return only columns from the left data frame") {
    val leftSemiJoin = runJoin(MyJoinType.LEFT_SEMI)
    val count = leftSemiJoin.count()
    leftSemiJoin.show
    leftSemiJoin.printSchema()
    val expectedColumns = Array("band", "guitars", "id", "name")
    val columns = leftSemiJoin.columns
    assertResult(3L)(count)
    assertResult(expectedColumns)(columns)
  }
  test("a left anti join should return only rows of the right table that do not satisfy the left join condition") {
    val leftAntiJoin = runJoin(MyJoinType.LEFT_ANTI)
    val count = leftAntiJoin.count()
    leftAntiJoin.show
    leftAntiJoin.printSchema()
    val expectedColumns = Array("band", "guitars", "id", "name")
    val columns = leftAntiJoin.columns
    assertResult(1L)(count)
    assertResult(expectedColumns)(columns)
  }
  test("selecting ambiguous columns should throw a runtime exception") {
    try {
      val runtimeException = selectWithoutRenaming()
      runtimeException.show()
    } catch {
      case msg: Throwable =>
        println(msg)
        assert(msg.toString.contains("AnalysisException"))
      case _ => fail("the expected Throwable exception was not AnalysisException")
    }
  }
  test("selecting with drop duplicated column should not throw a runtime exception") {
    try {
      val runtimeException = selectWithDropingDuplicatedColumns()
      runtimeException.printSchema()
      runtimeException.show()
      val expectedColumns = Array("band", "guitars", "id", "name", "hometown", "band_name", "year")
      val columns = runtimeException.columns
      assertResult(expectedColumns)(columns)
    } catch {
      case _ => fail("the expected Throwable exception was not AnalysisException")
    }
  }
  test("a join with complex expression") {
    val df = runJoinWithComplexExpression(MyJoinType.INNER_JOIN)
    df.printSchema()
    df.show()
    val count = df.count()
    assertResult(5L)(count)
  }
}
