package com.barnwaldo.kafkaQueues;

import com.barnwaldo.kafkaQueues.model.DataRecord;
import com.barnwaldo.kafkaQueues.utils.QueueReader;
import com.barnwaldo.kafkaQueues.utils.Receiver;
import com.barnwaldo.kafkaQueues.utils.Sender;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class KafkaQueuesApplication implements CommandLineRunner {

    final int numRecordsToGenerate = 10000000;
    final int numQueuesForListening = 20000;
    final String topic = "data";
	Random random = new Random();
    ObjectMapper mapper = new ObjectMapper();
    Map<String, BlockingQueue<DataRecord>> dataQueues = new ConcurrentHashMap<>();
    Map<String, Long> totals = new ConcurrentHashMap<>();
    Map<String, Long> sortedTotals = new TreeMap<>();

	public static void main(String[] args) {
        SpringApplication.run(KafkaQueuesApplication.class, args);
	}

    @Override
    public void run(String... args) throws Exception {
        // start Kafka Consumer Thread
        Receiver receiver = new Receiver(topic, dataQueues);
        Thread tReceiver = new Thread(receiver);
        tReceiver.start();

        QueueReader queueReader = new QueueReader(dataQueues, totals);
        Thread tReader = new Thread(queueReader);
        tReader.start();

        generateDataRecords(numRecordsToGenerate, numQueuesForListening);
        try{
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException ex) {
            System.out.println(ex.getMessage());
        }
        receiver.interrupt();
        queueReader.interrupt();
        queueReader.getTotals().forEach((k,v)-> sortedTotals.put(k,v));
        System.out.println("Records received by Consumer and Processed: " + queueReader.getProcessed());
        System.out.println("Number of queues: " + sortedTotals.size());
        System.out.println("Sample of queue records processed: ");
        sortedTotals.forEach((k,v)-> {
            if(Integer.parseInt(k.substring(5)) % 1000 == 0) System.out.println("  --> Queue : " + k + " has processed : " + v + " records...");
        });
    }

	// private helper method to generate random data records and send through Kafka Producer
	private void generateDataRecords(int numRecords, int numQueues) {
        Sender sender = new Sender(topic);
		for (int i = 0; i < numRecords; i++) {
			// create data record
			String key = "queue" +  String.format("%06d", random.nextInt(numQueues));
			DataRecord record = new DataRecord(i, key, randomString(12), random.nextInt(10000), random.nextDouble());
			// send record to Kafka Producer
            try{
                String value = mapper.writeValueAsString(record);
                sender.sendRecord(key, value);
            } catch (JsonProcessingException ex) {
                System.out.println(ex.getMessage());
            }
		}
	}

	// private helper method - random string generator
	private String randomString(final int len) {
		final StringBuilder sb = new StringBuilder();
		final String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
		final int strlen = alphabet.length();

		for (int i = 0; i < len; ++i) {
			sb.append(alphabet.charAt(random.nextInt(strlen)));
		}
		return sb.toString();
	}


}