name := "explore-spark"

version := "0.1"

scalaVersion := "2.12.3"

val sparkVersion = "3.0.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.twitter" %% "algebird-core" % "0.13.7",
  "joda-time" % "joda-time" % "2.5",
  "org.fusesource.mqtt-client" % "mqtt-client" % "1.16"
)

mainClass in(Compile, packageBin) := Some("org.sense.spark.app.TaxiRideCountCombineByKey")
