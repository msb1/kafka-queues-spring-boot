package com.barnwaldo.kafkaQueues.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;


public class Sender {

    private final String topic;
    private final KafkaProducer<String, String> producer;
    private ObjectMapper mapper = new ObjectMapper();

    public Sender(String topic) {
        this.topic = topic;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.21.10:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        producer = new KafkaProducer<String, String>(props);
    }

    public void sendRecord(String key, String message) {
        // send record to Kafka Producer
        final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
        Future<RecordMetadata> future = producer.send(record);
    }
}
