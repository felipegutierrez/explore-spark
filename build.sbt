name := "explore-spark"

version := "0.3"

scalaVersion := "2.12.7"

val sparkVersion = "3.0.0"

// remove "provided" flag in order to test using the Intellij IDEA
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion % "provided",
  "io.dropwizard.metrics" % "metrics-core" % "4.1.11" % "provided",
  "com.twitter" %% "algebird-core" % "0.13.7",
  "joda-time" % "joda-time" % "2.5",
  "org.fusesource.mqtt-client" % "mqtt-client" % "1.16"
)

assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter { f =>
    f.data.getName.contains("spark") ||
      f.data.getName == "spark-streaming_2.12-3.0.0.jar" ||
      f.data.getName == "spark-sql_2.12-3.0.0.jar" ||
      f.data.getName == "spark-streaming-kafka-0-10_2.12-3.0.0.jar"
  }
}

mainClass in(Compile, packageBin) := Some("org.sense.spark.app.App")
mainClass in assembly := Some("org.sense.spark.app.App")

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := s"${name.value}_${scalaBinaryVersion.value}-fat_${version.value}.jar"
