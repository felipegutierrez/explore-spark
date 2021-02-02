package org.github.explore.spark.dataframes

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, max, col}

object DataFrameJoins {

  val spark = SparkSession.builder()
    .appName(DataFrameJoins.getClass.getSimpleName)
    .config("spark.master", "local")
    .getOrCreate()

  // Reading from a remote DB
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  val guitarsDF: sql.DataFrame = getDataFrameFromJson("src/main/resources/data/guitars.json")
  val guitaristsDF: sql.DataFrame = getDataFrameFromJson("src/main/resources/data/guitarPlayers.json")
  val bandsDF: sql.DataFrame = getDataFrameFromJson("src/main/resources/data/bands.json")

  def getDataFrameFromJson(pathJson: String): sql.DataFrame = {
    spark.read
      .option("inferSchema", "true")
      .json(pathJson)
  }

  def main(args: Array[String]): Unit = {
    run()
  }

  def run() = {
    //    val guitaristsBandDF = runJoin(MyJoinType.INNER_JOIN)
    //    guitaristsBandDF.show
    //    val allGuitaristsBandDF = runJoin(MyJoinType.LEFT_OUTER)
    //    allGuitaristsBandDF.show
    //    val guitaristsAllBandDF = runJoin(MyJoinType.RIGHT_OUTER)
    //    guitaristsAllBandDF.show
    //    val allGuitaristsAllBandDF = runJoin(MyJoinType.OUTER)
    //    allGuitaristsAllBandDF.show
    //    val leftSemiJoin = runJoin(MyJoinType.LEFT_SEMI)
    //    leftSemiJoin.show
    //    val leftAntiJoin = runJoin(MyJoinType.LEFT_ANTI)
    //    leftAntiJoin.show
    //    val runtimeException = selectWithoutRenaming()
    //    runtimeException.show()

    // allEmployeesAndMaxSalaries()
    // allEmployeesAndMaxSalariesFaster()
    // allNotManagerEmployees()
    bestPaiedEmployees()
  }

  /**
   * 3. find the job titles of the best paid 10 employees in the company
   */
  def bestPaiedEmployees() = {
    val titlesDF = readFromPostgreSql("titles")
    val employeesDF = readFromPostgreSql("employees")
    val salariesDF = readFromPostgreSql("salaries")

    val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no", "title").agg(max("to_date"))
    val maxSalariesPerEmpNoDF = salariesDF.groupBy("emp_no").agg(max("salary").as("maxSalary"))

    val joinCondition = employeesDF.col("emp_no") === maxSalariesPerEmpNoDF.col("emp_no")
    val bestSalariesEmployees = employeesDF.join(maxSalariesPerEmpNoDF, joinCondition)
      .drop(maxSalariesPerEmpNoDF.col("emp_no"))
      .orderBy(col("maxSalary").desc_nulls_last)
      .limit(10)
    val result = bestSalariesEmployees.join(mostRecentJobTitlesDF, "emp_no")
    result.show()
  }

  /**
   * 2. show all employees who were never managers
   * select * from employees as e join titles as t on (e.emp_no = t.emp_no) where title != 'Manager';
   */
  def allNotManagerEmployees() = {
    val employeesDF = readFromPostgreSql("employees")
    // val titlesDF = readFromPostgreSql("titles")
    val deptManagerDF = readFromPostgreSql("dept_manager")
    val joinCondition = employeesDF.col("emp_no") === deptManagerDF.col("emp_no")
    val result = employeesDF.join(deptManagerDF, joinCondition, MyJoinType.LEFT_ANTI)
    // .filter(col("title") =!= "Manager")
    // .filter(col("first_name") === "Oscar" and col("last_name") === "Ghazalie")
    result.printSchema()
    result.show()
  }

  def allEmployeesAndMaxSalaries() = {
    val employeesAndSalaries = allEmployeesAndSalaries(MyJoinType.INNER_JOIN)
    employeesAndSalaries.printSchema()
    employeesAndSalaries.show()
    val maxSalaries = employeesAndSalaries
      .select("emp_no", "first_name", "last_name", "salary")
      .groupBy("emp_no", "first_name", "last_name")
      .max("salary")
    maxSalaries.printSchema()
    maxSalaries.show()
  }

  /**
   * 1. show all employees and their max salary
   */
  def allEmployeesAndSalaries(joinType: String): sql.DataFrame = {
    val employeesDF = readFromPostgreSql("employees")
    val salariesDF = readFromPostgreSql("salaries")
    val joinCondition = employeesDF.col("emp_no") === salariesDF.col("emp_no")
    employeesDF.join(salariesDF, joinCondition, joinType)
      .drop(salariesDF.col("emp_no"))
  }

  def readFromPostgreSql(postgreSqlTable: String = "employees"): sql.DataFrame = {
    val dataframe = spark.read
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", s"public.$postgreSqlTable")
      .load()
    dataframe
  }

  def selectWithoutRenaming(): sql.DataFrame = {
    val guitaristsBandDF = runJoin(MyJoinType.INNER_JOIN)
    guitaristsBandDF.select("id", "band")
  }

  def runJoin(joinType: String): sql.DataFrame = {
    val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
    guitaristsDF.join(bandsDF, joinCondition, joinType)
  }

  /**
   * 1. show all employees and their max salary
   */
  def allEmployeesAndMaxSalariesFaster() = {
    val employeesDF = readFromPostgreSql("employees")
    val salariesDF = readFromPostgreSql("salaries")
      .groupBy("emp_no").max("salary")

    val joinCondition = employeesDF.col("emp_no") === salariesDF.col("emp_no")
    val maxSalaries = employeesDF.join(salariesDF, joinCondition, MyJoinType.INNER_JOIN)
    maxSalaries.printSchema()
    maxSalaries.show()
  }

  def selectWithDropingDuplicatedColumns(): sql.DataFrame = {
    val guitaristsBandDF = runJoinWithRenaming(MyJoinType.INNER_JOIN)
    guitaristsBandDF
      .drop(bandsDF.col("id"))
      .select("band", "guitars", "id", "name", "hometown", "band_name", "year")
  }

  def runJoinWithRenaming(joinType: String): sql.DataFrame = {
    val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
    guitaristsDF.join(bandsDF.withColumnRenamed("name", "band_name"), joinCondition, joinType)
  }

  def runJoinWithComplexExpression(joinType: String): sql.DataFrame = {
    guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"),
      expr("array_contains(guitars, guitarId)"),
      joinType)
  }

  object MyJoinType extends Enumeration {
    val INNER_JOIN = "inner"
    val LEFT_OUTER = "left_outer"
    val RIGHT_OUTER = "right_outer"
    val OUTER = "outer"
    val LEFT_SEMI = "left_semi"
    val LEFT_ANTI = "left_anti"
  }

}
