# explore-spark

### Requirements
Install JDK 1.8, Scala 2.12.3, and sbt 1.3.12. The project is using Spark 3.0.0

### Compile and Execute the project
```
sbt compile
sbt run
```
### Create the jar file of the project
```
sbt package
```
### Configuring Prometheus on file `/etc/prometheus/prometheus.yml` to scrape metrics from Spark:
```
scrape_configs:
  - job_name: "spark_streaming_app"
    scrape_interval: "5s"
    static_configs:
      - targets: ['localhost:8082']
```
### Deploy in a Spark standalone cluster and exporting metrics using JMX and Prometheus library:

After starting the Spark standalone cluster access it at [http://127.0.0.1:8080/](http://127.0.0.1:8080/) and Prometheus console at [http://127.0.0.1:9090/graph](http://127.0.0.1:9090/graph)
```
$ cd /home/flink/spark-3.0.0-bin-hadoop2.7/
$ sbin/start-all.sh 
$ ./bin/spark-submit --master spark://127.0.0.1:7077 --deploy-mode cluster --driver-cores 4 --name "WordCountStreamCombineByKey" \
    --conf "spark.driver.extraJavaOptions=-javaagent:/home/flink/spark-3.0.0-bin-hadoop2.7/jars/jmx_prometheus_javaagent-0.13.0.jar=8082:/home/flink/spark-3.0.0-bin-hadoop2.7/conf/spark.yml" \
    /home/felipe/workspace-idea/explore-spark/target/scala-2.11/explore-spark_2.11-0.1.jar
```




