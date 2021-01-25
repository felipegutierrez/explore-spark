name := "explore-spark"
version := "0.4"
scalaVersion := "2.12.7"

val sparkVersion = "3.0.0"
val log4jVersion = "2.4.1"
val dropwizardVersion = "4.1.11"
val twitterVersion = "0.13.7"
val jodaVersion = "2.5"
val fusesourceVersion = "1.16"
val vegasVersion = "0.3.11"
val postgresVersion = "42.2.2"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

// remove "provided" flag in order to test using the Intellij IDEA
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion, // % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion, // % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion, // % "provided",

  // logging
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,

  // postgres for DB connectivity
  "org.postgresql" % "postgresql" % postgresVersion,

  // dropwizard
  "io.dropwizard.metrics" % "metrics-core" % dropwizardVersion, // % "provided",

  // twitter
  "com.twitter" %% "algebird-core" % "0.13.7",

  // joda
  "joda-time" % "joda-time" % jodaVersion,

  // mqtt
  "org.fusesource.mqtt-client" % "mqtt-client" % fusesourceVersion,
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

mainClass in(Compile, packageBin) := Some("org.github.explore.App")
mainClass in assembly := Some("org.github.explore.App")

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := s"${name.value}_${scalaBinaryVersion.value}-fat_${version.value}.jar"
