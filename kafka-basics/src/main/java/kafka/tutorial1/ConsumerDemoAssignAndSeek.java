package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignAndSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class.getName());

        String bootstrapServer = "127.0.0.1:9092";
        String topic = "first_topic";

        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(consumerProperties);
        long offsetToReadFrom =15L;
        TopicPartition topicPartitionToReadFrom = new TopicPartition(topic,0);
        consumer.assign(Collections.singletonList(topicPartitionToReadFrom));
        consumer.seek(topicPartitionToReadFrom,offsetToReadFrom);
        boolean keepReading = true;

        int numOfMessagesToRead = 5;
        int numOfMessagesReadSoFar = 0;

        while(keepReading) {
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> record : records) {
                numOfMessagesReadSoFar ++;
                logger.info("Key = " + record.key() + " " + "Value = " + record.value());
                logger.info(" Partition = " + record.partition() + " Offset = " + record.offset() + "\n");
                if(numOfMessagesReadSoFar >=  numOfMessagesToRead)
                {
                    keepReading = false;
                    break;
                }
            }
        }
    }
}


