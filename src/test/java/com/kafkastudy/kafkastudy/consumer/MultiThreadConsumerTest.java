package com.kafkastudy.kafkastudy.consumer;

import com.kafkastudy.kafkastudy.consumer.ConsumerWorker;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SpringBootTest
class MultiThreadConsumerTest {
    private String TOPIC_NAME = "test_topic";
    private String GROUP_ID = "group-id-test";
    private String BOOTSTRAP_SERVERS = "http://localhost:9092";
    private int CONSUMER_COUNT = 3;
    private static List<ConsumerWorker> workerThreads = new ArrayList<>();

    /**
     * 1. 하나의 프로세스에서 3개의 쓰레드가 컨슈밍한다.
     * 2. 런타임 종료(wakeup 라는 메세지를 소비할 때)시 무한 polling을 탈출시키는 wakeup()을 실행한다.
     * 3. 안전하게 쓰레드를 닫고 커넥션을 닫고 종료한다.
     * 4. 로그를 확인한다.
     *
     * @throws InterruptedException
     */
    @Test
    void multi_thread_consumer_wake_up_test() throws InterruptedException {
        Runtime.getRuntime().addShutdownHook(new ShutdownThread());
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        CountDownLatch latch = new CountDownLatch(1);
        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i = 0; i < CONSUMER_COUNT; i++) {
            ConsumerWorker worker = new ConsumerWorker(configs, TOPIC_NAME, i, latch);
            workerThreads.add(worker);
            executorService.execute(worker);
        }

        latch.await();
    }

    static class ShutdownThread extends Thread {
        public void run() {
            workerThreads.forEach(ConsumerWorker::shutdown);
            System.out.println("Safely close");
        }
    }
}
