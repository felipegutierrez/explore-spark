name := "explore-spark"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.1.0",
  "com.twitter" %% "algebird-core" % "0.8.0"
)
