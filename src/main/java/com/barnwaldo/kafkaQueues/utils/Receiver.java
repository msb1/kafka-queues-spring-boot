package com.barnwaldo.kafkaQueues.utils;

import com.barnwaldo.kafkaQueues.model.DataRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;


public class Receiver implements Runnable {

    private final String topic;
    private final KafkaConsumer<String, String> consumer;
    private BlockingQueue<DataRecord> queue;
    private ObjectMapper mapper;
    private Duration timeout;

    private long received = 0;
    private AtomicBoolean running = new AtomicBoolean(false);
    private DataRecord dataRecord = new DataRecord();
    private Map<String, BlockingQueue<DataRecord>> dataQueues;

    public long getReceived() {
        return received;
    }

    public Map<String, BlockingQueue<DataRecord>> getDataQueues() {
        return dataQueues;
    }

    public Receiver(String topic, Map<String, BlockingQueue<DataRecord>> dataQueues) {
        this.topic = topic;
        this.dataQueues = dataQueues;
        this.mapper = new ObjectMapper();
        this.timeout = Duration.ofMillis(1000);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.21.10:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "barnwaldo");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);

        consumer = new KafkaConsumer<>(props);
    }

    public void interrupt() {
        running.set(false);
    }

    boolean isRunning() {
        return running.get();
    }

    @Override
    public void run() {

        running.set(true);
        consumer.subscribe(Collections.singletonList(topic));
        System.out.println("Kafka Receiver Thread running for topic: " + topic);
        while (running.get()) {
            ConsumerRecords<String, String> consumerRecords =  consumer.poll(timeout);
            for (ConsumerRecord<String, String>  consumerRecord : consumerRecords) {
                received += 1;
                if (received % 10000 == 0) {
                    System.out.println("Records received: " + received + "-- current record: " + consumerRecord.value());
                }
                try{
                    dataRecord = mapper.readValue(consumerRecord.value(), DataRecord.class);
                } catch (IOException ex) {
                    System.out.println(ex.getMessage());
                }
                try{
                    if(dataQueues.containsKey(consumerRecord.key())) {
                        BlockingQueue<DataRecord> temp = dataQueues.get(consumerRecord.key());
                        temp.put(dataRecord);
                    } else {
                        BlockingQueue<DataRecord> temp = new ArrayBlockingQueue<>(4096);
                        temp.put(dataRecord);
                        dataQueues.put(consumerRecord.key(), temp);
                    }
                } catch (InterruptedException ex) {
                    System.out.println(ex.getMessage());
                }
            }
        }
        consumer.close();
    }
}