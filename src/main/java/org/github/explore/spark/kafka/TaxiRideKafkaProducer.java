package org.github.explore.spark.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.github.explore.spark.util.DataRateListener;
import org.github.explore.spark.util.Utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

public class TaxiRideKafkaProducer {

    private final static int DEFAULT_MAX_COUNT = 5;
    private final static String TOPIC = Utils.TOPIC_KAFKA_TAXI_RIDE();
    private final static String BOOTSTRAP_SERVERS = "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094";
    private final DataRateListener dataRateListener;

    public TaxiRideKafkaProducer() {
        this(DEFAULT_MAX_COUNT);
    }

    public TaxiRideKafkaProducer(int maxCount) {
        System.out.println("TaxiRideKafkaProducer");
        dataRateListener = new DataRateListener();
        dataRateListener.start();
        if (maxCount == 0) {
            throw new RuntimeException("If you start the Kafka producer you may want to run it at least one.\n" +
                    "use: -count [>0|-1==infinite]");
        } else {
            runProducer(maxCount);
        }
    }

    private Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private void runProducer(final int sendMessageCount) {
        final Producer<Long, String> producer = createProducer();
        try {
            int count = 0;
            InputStream gzipStream = new GZIPInputStream(new FileInputStream("/home/flink/nycTaxiRides.gz"));
            BufferedReader reader = new BufferedReader(new InputStreamReader(gzipStream, StandardCharsets.UTF_8));
            String line;
            long startTime;
            while ((sendMessageCount == -1 || count < sendMessageCount)
                    && reader.ready() && (line = reader.readLine()) != null) {
                count++;
                startTime = System.nanoTime();

                final ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, startTime, line);
                RecordMetadata metadata = producer.send(record).get();

                // System.out.printf("sent record(key=%s value=%s) meta(partition=%d, offset=%d) time=%d\n", record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
                // sleep in nanoseconds to have a reproducible data rate for the data source
                dataRateListener.busySleep(startTime);
            }
            reader.close();
            gzipStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.flush();
            producer.close();
        }
    }
}
