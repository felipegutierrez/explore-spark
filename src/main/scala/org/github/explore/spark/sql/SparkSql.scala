package org.github.explore.spark.sql

import org.apache.spark.sql
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkSql {

  val spark = SparkSession.builder()
    .appName("SparkSqlPractice")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    // only for Spark 2.4 users:
    // .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // transfer tables from a DB to Spark tables
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def selectCarsFromUSA(): sql.DataFrame = {
    // use Spark SQL
    carsDF.createOrReplaceTempView("cars")
    val americanCarsDF = spark.sql(
      """
        |select Name from cars where Origin = 'USA'
    """.stripMargin)
    americanCarsDF
  }

  def main(args: Array[String]): Unit = {
    run()
  }

  def run() = {
    // we can run ANY SQL statement
    spark.sql("create database rtjvm")
    spark.sql("use rtjvm")
    val databasesDF = spark.sql("show databases")

    transferTables(List(
      "employees",
      "departments",
      "titles",
      "dept_emp",
      "salaries",
      "dept_manager"),
      false)

    // read DF from loaded Spark tables
    val employeesDF2 = spark.read.table("employees")
    employeesDF2.show

    /** 1. Read the movies DF and store it as a Spark table in the rtjvm database. */
    transferTables(List("movies"), false)

    val employees = countEmployees()
    employees.show
    println(s"count: ${employees.count}")

    val averageSalaries = averageSalaryOfEmployees()
    averageSalaries.show

    bestPaymentDepartments.show
  }

  def transferTables(tableNames: List[String], shouldWriteToWarehouse: Boolean = false) = tableNames.foreach { tableName =>
    val tableDF = readTable(tableName)
    tableDF.createOrReplaceTempView(tableName)

    if (shouldWriteToWarehouse) {
      tableDF.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    }
  }

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  /**
   * Exercises
   *
   * 1. Read the movies DF and store it as a Spark table in the rtjvm database.
   * 2. Count how many employees were hired in between Jan 1 1999 and Jan 1 2000.
   * 3. Show the average salaries for the employees hired in between those dates, grouped by department.
   * 4. Show the name of the best-paying department for employees hired in between those dates.
   */

  /** 3. Show the average salaries for the employees hired in between those dates, grouped by department. */
  def averageSalaryOfEmployees(): sql.DataFrame = {
    val salaries = spark.sql(
      """
        |SELECT d.dept_no, AVG(s.salary) as average_salary
        |FROM employees e, salaries s, dept_emp d
        |WHERE
        |    e.emp_no = s.emp_no AND e.emp_no = d.emp_no
        |    AND CAST(hire_date AS date) >= CAST('1999-01-01' AS date)
        |    AND CAST(hire_date AS date) <= CAST('2000-01-01' AS date)
        |GROUP BY (d.dept_no)
        |""".stripMargin)
    salaries
  }

  /** 4. Show the name of the best-paying department for employees hired in between those dates. */
  def bestPaymentDepartments(): sql.DataFrame = {
    val bestPayments = spark.sql(
      """
        |SELECT d.dept_no, de.dept_name, AVG(s.salary) as average_salary
        |FROM employees e, salaries s, dept_emp d, departments de
        |WHERE
        |    e.emp_no = s.emp_no AND e.emp_no = d.emp_no
        |    AND de.dept_no = d.dept_no
        |    AND CAST(hire_date AS date) >= CAST('1999-01-01' AS date)
        |    AND CAST(hire_date AS date) <= CAST('2000-01-01' AS date)
        |GROUP BY d.dept_no, de.dept_name
        |ORDER BY average_salary DESC
        |LIMIT 1
        |""".stripMargin)
    bestPayments
  }

  /** 2. Count how many employees were hired in between Jan 1 1999 and Jan 1 2000. */
  def countEmployees(): sql.DataFrame = {
    val employees = spark.sql(
      """
        |select * from employees
        |where
        |   CAST(hire_date AS date) >= CAST('1999-01-01' AS date) AND
        |   CAST(hire_date AS date) <= CAST('2000-01-01' AS date)
        |""".stripMargin)
    employees
  }
}
