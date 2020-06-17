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

After starting the Spark standalone cluster access it at [http://127.0.0.1:8080/](http://127.0.0.1:8080/)
```
$ cd spark-2.4.6-bin-hadoop2.7/
$ sbin/start-all.sh 
$ ./bin/spark-submit --master spark://localhost:7077 --deploy-mode cluster \
      --driver-cores 4 --name "TaxiRideCountCombineByKey" \
      target/scala-2.11/explore-spark_2.11-0.1.jar
```

