package org.github.explore.spark

import org.apache.spark.sql.{SQLImplicits, SparkSession}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SharedSparkSession extends BeforeAndAfterAll {
  self: Suite =>

  /**
   * A helper implicit value that allows us to import SQL implicits.
   */
  protected lazy val sqlImplicits: SQLImplicits = self.sparkSession.implicits
  /**
   * The SparkSession instance to use for all tests in one suite.
   */
  private var spark: SparkSession = _

  /**
   * Runs before all tests and starts spark session.
   */
  override def beforeAll(): Unit = {
    startSparkSession()
    super.beforeAll()
  }

  /**
   * Starts a new local spark session for tests.
   */
  protected def startSparkSession(): Unit = {
    if (spark == null) {
      spark = SparkSession
        .builder()
        .master("local[2]")
        .appName("Testing Spark Session")
        .getOrCreate()
    }
  }

  /**
   * Runs after all tests and stops existing spark session.
   */
  override def afterAll(): Unit = {
    super.afterAll()
    stopSparkSession()
  }

  /**
   * Stops existing local spark session.
   */
  protected def stopSparkSession(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
  }

  /**
   * Returns local running SparkSession instance.
   *
   * @return SparkSession instance `spark`
   */
  protected def sparkSession: SparkSession = spark
}
