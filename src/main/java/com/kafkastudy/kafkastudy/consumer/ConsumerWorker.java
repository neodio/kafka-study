package com.kafkastudy.kafkastudy.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerWorker implements Runnable {
    private Properties prop;
    private String topic;
    private String threadName;
    private KafkaConsumer<String, String> consumer;
    private CountDownLatch countDownLatch;

    ConsumerWorker(Properties prop, String topic, int number, CountDownLatch countDownLatch) {
        this.prop = prop;
        this.topic = topic;
        this.threadName = "consumer-thread-" + number;
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        consumer = new KafkaConsumer<>(prop);
        consumer.subscribe(Arrays.asList(topic));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(threadName + " >> " + record.value());
                    if (record.value().equals("wakeup")) {
                        countDownLatch.countDown();
                    }
                }
                consumer.commitSync();
            }
        } catch (WakeupException e) {
            System.out.println(threadName + " trigger WakeupException");
        } finally {
            consumer.commitSync();
            consumer.close();
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }
}