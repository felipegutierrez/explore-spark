package org.github.explore.spark.streaming.structure

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.{LongOffset, MemoryStream}
import org.github.explore.spark.SharedSparkSession
import org.scalatest.funsuite.AnyFunSuite

class StreamingDataFramesSpec extends AnyFunSuite with SharedSparkSession {

  test("spark structured streaming can read from directory") {

    val queryName: String = "csvquery"
    val df = StreamingDataFrames.readData(sparkSession, "csv")
    val streamingQuery = StreamingDataFrames.writeData(df, "memory", queryName)
    streamingQuery.awaitTermination(3000L)
    val result: DataFrame = sparkSession.table(queryName)
    result.show

    assertResult(560)(result.count)
  }

  test("spark structured streaming can write to memory socket") {
    implicit val sqlCtx = sparkSession.sqlContext
    import sqlImplicits._

    val events = MemoryStream[String]
    val queryName: String = "calleventaggs"

    // Add events to MemoryStream as if they came from Kafka
    val batch = Seq(
      "this is a value to read",
      "and this is another value"
    )
    val currentOffset = events.addData(batch)

    val streamingQuery = StreamingDataFrames.writeData(events.toDF(), "memory", queryName)

    streamingQuery.processAllAvailable()
    events.commit(currentOffset.asInstanceOf[LongOffset])

    val result: DataFrame = sparkSession.table(queryName)
    result.show

    streamingQuery.awaitTermination(1000L)
    assertResult(batch.size)(result.count)

    val values = result.take(2)
    assertResult(batch(0))(values(0).getString(0))
    assertResult(batch(1))(values(1).getString(0))
  }
}
