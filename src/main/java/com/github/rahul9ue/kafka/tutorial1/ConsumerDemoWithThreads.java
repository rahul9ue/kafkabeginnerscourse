package com.github.rahul9ue.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {
    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();

    }

    private ConsumerDemoWithThreads() {

    }

    private void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());

        String bootstrapServer = "127.0.0.1:9092";
        String topic = "first_topic";
        String groupId = "my-consumerDemoWithThreads-application";

        CountDownLatch latch = new CountDownLatch(1);
        logger.info("Creating the consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(bootstrapServer,groupId,topic,latch);
        Thread myConsumerThread = new Thread(myConsumerRunnable);
        myConsumerThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
                logger.info("Caught Shuttdown Hook");
                ((ConsumerRunnable)myConsumerRunnable).shutdown();
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                logger.info("Application has exited");
            }
            )
        );

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
        private KafkaConsumer<String,String> consumer;
        private CountDownLatch countDownLatch;

        private ConsumerRunnable(String bootstrapServer, String groupId, String topic, CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
            Properties consumerProperties = new Properties();
            consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            consumer = new KafkaConsumer<String,String>(consumerProperties);

            consumer.subscribe(Collections.singletonList(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key = " + record.key() + " " + "Value = " + record.value());
                        logger.info(" Partition = " + record.partition() + " Offset = " + record.offset() + "\n");
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received Shutdown Signal");
            }
            finally {
                consumer.close();
                countDownLatch.countDown();
            }
        }

        public void shutdown () {
            consumer.wakeup();
        }
    }
}


