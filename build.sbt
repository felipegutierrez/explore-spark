name := "explore-spark"
version := "0.5"
scalaVersion := "2.12.7"

val scalaTestVersion = "3.2.2"
val sparkVersion = "3.0.1"
val log4jVersion = "2.4.1"
val dropwizardVersion = "4.1.11"
val twitterAlgebirdVersion = "0.13.7"
val jodaVersion = "2.5"
val fusesourceVersion = "1.16"
val vegasVersion = "0.3.11"
val postgresVersion = "42.2.2"
val cassandraConnectorVersion = "3.0.0" // preview version at the moment of writing (July 7, 2020)
val kafkaVersion = "2.4.0"
val nlpLibVersion = "3.5.1"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

// ########## Spark unit tests configurations ##########
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled") // increase memory for
parallelExecution in Test := false // disable parallelism to
// ########## Spark unit tests configurations ##########

// remove "provided" flag in order to test using the Intellij IDEA
libraryDependencies ++= Seq(
  // Spark
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion, // % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion, // % "provided",

  // streaming-kafka
  "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % sparkVersion,
  // low-level integrations
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kinesis-asl" % sparkVersion,

  // kafka
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion,

  // cassandra
  "com.datastax.spark" %% "spark-cassandra-connector" % cassandraConnectorVersion,

  // Scala test
  "org.scalatest" %% "scalatest" % scalaTestVersion,
  "junit" % "junit" % "4.13" % Test,

  // logging
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,

  // postgres for DB connectivity
  "org.postgresql" % "postgresql" % postgresVersion,

  // dropwizard metrics
  "io.dropwizard.metrics" % "metrics-core" % dropwizardVersion, // % "provided",

  // twitter
  "com.twitter" %% "algebird-core" % twitterAlgebirdVersion,

  // joda
  "joda-time" % "joda-time" % jodaVersion,

  // MQTT broker
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
