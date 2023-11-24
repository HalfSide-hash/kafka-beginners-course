package io.conduktor.demos.kafka;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        System.out.println("I MUST CONSUME");

        //Define Properties
        Properties props = new Properties();
        //props.setProperty("bootstrap.servers","127.0.0.1:9092");
        props.setProperty("bootstrap.servers", "robust-dolphin-11335-us1-kafka.upstash.io:9092");
        props.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        props.setProperty("security.protocol", "SASL_SSL");
        props.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"cm9idXN0LWRvbHBoaW4tMTEzMzUkHoNnwD9kHehCFQvMy6kmwkpycaBeng-BZu0\" password=\"YTE4YWNjMjYtNzIzZC00M2U1LTkyOTEtOGM5MjM0YzU2NWIy\";");

        //for consumers
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", StringDeserializer.class.getName());




    }
}
