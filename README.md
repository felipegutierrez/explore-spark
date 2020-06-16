# explore-spark

### Requirements
Install JDK 1.8, Scala 2.11, and sbt 1.3.12

### Compile and Execute the project
```
sbt compile
sbt run
```
### Create the jar file of the project
```
sbt package
```
### Deploy in a Spark standalone cluster
```
./bin/spark-submit --master spark://localhost:7077 --deploy-mode cluster \
--driver-cores 4 --name "TestStreamCombineByKey" \
target/scala-2.11/explore-spark_2.11-0.1.jar
```

