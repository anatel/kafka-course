package com.github.anatel.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

        String bootstrapServers = "localhost:9092";

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i=0; i<10; i++) {
            String topic = "first_topic";
            String value = "hello world " + i;
            String key = "id_" + i;
            // create a producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, key, value);

            // send data
            logger.info("Key: " + key); // log the key
            producer.send(record, (recordMetadata, e) -> {
                // executes every time a record is successfully or an exception is thrown
                if (e == null) {
                    // the record was successfully sent
                    logger.info("Received new metadata.\n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("Error while producing", e);
                }
            }).get(); // block the .send() to make it synchronous - don't do this is production.
        }

        // flush and close producer
        producer.close();
    }
}
