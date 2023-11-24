package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        log.info("Kafka time bois");

        //Define Properties
        Properties props = new Properties();
        //props.setProperty("bootstrap.servers","127.0.0.1:9092");
        props.setProperty("bootstrap.servers", "robust-dolphin-11335-us1-kafka.upstash.io:9092");
        props.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        props.setProperty("security.protocol", "SASL_SSL");
        props.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"cm9idXN0LWRvbHBoaW4tMTEzMzUkHoNnwD9kHehCFQvMy6kmwkpycaBeng-BZu0\" password=\"YTE4YWNjMjYtNzIzZC00M2U1LTkyOTEtOGM5MjM0YzU2NWIy\";");
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());
        //props.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(props);

        for (int j = 0; j <= 2; j++) {
            for (int i = 0; i < 10; i++) {
                String topic = "yava_time";
                String key = "id_" + i;
                String value = "hello world " + i;
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic, key, value);

                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // executed every time a record is successfully sent or an exception is thrown
                        if (e == null) {
                            log.info("key : " + key + " | partition : " + metadata.partition());
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });

            }
        }


        producer.flush();

        producer.close();
    }
}
