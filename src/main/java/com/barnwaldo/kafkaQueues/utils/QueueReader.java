package com.barnwaldo.kafkaQueues.utils;

import com.barnwaldo.kafkaQueues.model.DataRecord;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class QueueReader implements Runnable {

    private long processed = 0;
    private AtomicBoolean running = new AtomicBoolean(false);

    private Map<String, BlockingQueue<DataRecord>> dataQueues;
    private Map<String, Long> totals;

    public long getProcessed() {
        return processed;
    }

    public Map<String, Long> getTotals() {
        return totals;
    }

    public QueueReader(Map<String, BlockingQueue<DataRecord>> dataQueues, Map<String, Long> totals) {
        this.dataQueues = dataQueues;
        this.totals = totals;
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
        System.out.println("QueueReader processing queued records...");
        while (running.get()) {
            dataQueues.forEach((key, val) -> {
                if (!totals.containsKey(key)) {
                    totals.put(key, 0L);
                }
                long ctr = totals.get(key);
                for (int i = 0; i < val.size(); i++) {
                    // can do any record processing here
                    DataRecord r = val.poll();
                    processed += 1;
                    ctr += 1;
                }
                totals.put(key, ctr);
            });

            try{
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException ex) {
                System.out.println(ex.getMessage());
            }
        }
    }
}
