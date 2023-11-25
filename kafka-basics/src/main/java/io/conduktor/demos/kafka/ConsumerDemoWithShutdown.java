package io.conduktor.demos.kafka;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class ConsumerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {
        System.out.println("I MUST CONSUME");

        String groupId = "my-java-application";
        String topic = "yava_time";
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

        //adding group id
        props.setProperty("group.id", groupId);

        props.setProperty("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        final Thread mainThread = Thread.currentThread();


        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("shut that junk down");
                consumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try {
            consumer.subscribe(Arrays.asList(topic));

            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("key: " + record.key() + ", value: " + record.value());
                    log.info("Partition: " + record.partition() + ", offset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Lets shut down fr fr");
        } catch (Exception e){
            log.info("unexpected error: "+ e);
        } finally{
            consumer.close();
            log.info("consumer took that L gracefully");
        }

    }
}
