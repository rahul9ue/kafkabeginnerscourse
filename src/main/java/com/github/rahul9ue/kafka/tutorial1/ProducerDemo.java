package com.github.rahul9ue.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {

        String bootstrapServer = "127.0.1:9092";
        //create producer config
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String,String>  kafkaProducer = new KafkaProducer<String, String>(producerProperties);

        //create producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic","Hello World");

        //send data
        kafkaProducer.send(record);

        kafkaProducer.flush();

        kafkaProducer.close();
    }
}
