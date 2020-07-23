package org.sense.spark.app.combiners

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkEnv}
import org.fusesource.mqtt.client.QoS
import org.sense.spark.util._

object TaxiRideCountCombineByKey {

  val mqttTopic: String = Utils.TOPIC_MQTT_SINK
  val host: String = "127.0.0.1";
  val qos: QoS = QoS.AT_LEAST_ONCE

  def run(): Unit = {
    run("default", "default")
  }

  def run(input: String, output: String): Unit = {

    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 4 cores to prevent from a starvation scenario.
    val sparkConf = new SparkConf()
      .setMaster("local[4]")
      .set("spark.plugins", "org.sense.spark.util.CustomMetricSparkPlugin")
      .setAppName(TaxiRideCountCombineByKey.getClass.getSimpleName)

    val ssc = new StreamingContext(sparkConf, Milliseconds(1000))
    println("defaultParallelism: " + ssc.sparkContext.defaultParallelism)
    println("defaultMinPartitions: " + ssc.sparkContext.defaultMinPartitions)

    var evenCount = 0
    // UDFs
    val driverIdTaxiRideMap = (taxiRide: TaxiRide) => {
      evenCount = evenCount + 1
      CustomMetricSparkPlugin.value.inc(evenCount)
      (taxiRide.driverId, 1)
    }
    val combiner = (v: Int) => v
    val combinerMergeValue = (acc: Int, v: Int) => acc + v
    val taxiRideMap = (message: ConsumerRecord[String, String]) => {
      TaxiRideSource.getTaxiRideFromString(message.value())
    }

    val stream: DStream[TaxiRide] = input match {
      case Utils.VALUE_DEFAULT =>
        ssc.receiverStream(new TaxiRideSource()).cache()
      case Utils.VALUE_KAFKA =>
        val kafkaParams = Map[String, Object](
          "bootstrap.servers" -> "127.0.0.1:9092,192.168.0.28:9092",
          "key.deserializer" -> classOf[StringDeserializer],
          "value.deserializer" -> classOf[StringDeserializer],
          "group.id" -> "use_a_separate_group_id_for_each_stream",
          "auto.offset.reset" -> "latest",
          "enable.auto.commit" -> (false: java.lang.Boolean)
        )
        val topics = Array(Utils.TOPIC_KAFKA_TAXI_RIDE, Utils.TOPIC_KAFKA)
        val kafkaStream: InputDStream[ConsumerRecord[String, String]] =
          KafkaUtils.createDirectStream[String, String](
            ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams)
          )
        kafkaStream.map(taxiRideMap)
    }

    // DAG
    val driverStream: DStream[(Long, Int)] = stream.map(driverIdTaxiRideMap)
    val countStream: DStream[(Long, Int)] = driverStream
      .combineByKey(combiner, combinerMergeValue, combinerMergeValue, new HashPartitioner(4))

    // Emmit results
    output match {
      case Utils.VALUE_DEFAULT => countStream.print()
      case Utils.VALUE_MQTT =>
        println("Use the command below to consume data:")
        println("mosquitto_sub -h " + host + " -p 1883 -t " + mqttTopic)
        val mqttSink = ssc.sparkContext.broadcast(MqttSink(host))
        countStream.foreachRDD { rdd =>
          rdd.foreach { message =>
            mqttSink.value.send(mqttTopic, message.toString())
          }
        }
      case _ => println("Error: No output defined.")
    }

    SparkEnv.get.metricsSystem.report
    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}
