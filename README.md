# explore-spark

### Requirements
This project is using [Spark 3.0.0](https://spark.apache.org/releases/spark-release-3-0-0.html). The requirements are: JDK 1.8, Scala 2.12.7, and sbt 1.3.12.

### Compile and Execute the project
```
sbt compile
sbt run
```
### Create the jar file of the project
```
sbt package
sbt assembly // for fat jar files (with dependencies)
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
$ ./bin/spark-submit \
    --deploy-mode cluster \
    --driver-cores 4 \
    --conf spark.sql.shuffle.partitions=300 \
    --conf spark.default.parallelism=300 \
    --conf spark.streaming.backpressure.enabled=true \
    --conf spark.streaming.backpressure.initialRate=30 \
    --conf spark.streaming.receiver.maxRate=10000 \
    --conf spark.streaming.dynamicAllocation.enabled=true \
    --conf spark.streaming.kafka.maxRatePerPartition=100 \
    --jars /home/flink/spark-3.0.0-bin-hadoop2.7/jars/mqtt-client-1.16.jar,/home/flink/spark-3.0.0-bin-hadoop2.7/jars/hawtbuf-1.11.jar,/home/flink/spark-3.0.0-bin-hadoop2.7/jars/hawtdispatch-1.22.jar,/home/flink/spark-3.0.0-bin-hadoop2.7/jars/hawtdispatch-transport-1.22.jar,/home/flink/spark-3.0.0-bin-hadoop2.7/jars/kafka-clients-0.10.0.0.jar \
    --conf "spark.driver.extraJavaOptions=-javaagent:/home/flink/spark-3.0.0-bin-hadoop2.7/jars/jmx_prometheus_javaagent-0.13.0.jar=8082:/home/flink/spark-3.0.0-bin-hadoop2.7/conf/spark.yml" \
    --name "App" \
    /home/felipe/workspace-idea/explore-spark/target/scala-2.12/explore-spark_2.12-0.3.jar -app [1|2|3|4|5] -input [default|kafka] -output [default|mqtt]
```
After submitting the Spark application you will see its ID at the section "Running Drivers" on the web console. Only after submitting one application you also can see its job status at the web UI [http://127.0.0.1:4040/](http://127.0.0.1:4040/). Details of the Spark job "local-1595340043797" and its stages can be visualized at [http://127.0.0.1:4040/api/v1/applications/local-1595340043797/stages](http://127.0.0.1:4040/api/v1/applications/local-1595340043797/stages).

### Kafka producer

```
cd /home/felipe/Servers/kafka_2.13-2.5.0

# start zookeeper server
./bin/zookeeper-server-start.sh ./config/zookeeper.properties

# start broker
./bin/kafka-server-start.sh ./config/server.properties

# create topic "topic-kafka-taxi-ride"
./bin/kafka-topics.sh --create --topic topic-kafka-taxi-ride --zookeeper localhost:2181 --partitions 1 --replication-factor 1

# consume from the topic using the console producer
./bin/kafka-console-consumer.sh --topic topic-kafka-taxi-ride --bootstrap-server localhost:2181

# then launch Spark and the spark stream job
```


