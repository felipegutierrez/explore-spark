name := "explore-spark"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.bahir" %% "spark-streaming-twitter" % sparkVersion,
  "com.twitter" %% "algebird-core" % "0.8.0"
)

mainClass in(Compile, packageBin) := Some("org.sense.spark.app.TestStreamCombineByKey")
