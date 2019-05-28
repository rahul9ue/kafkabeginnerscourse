package com.github.rahul9ue.kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    private String consumerKey="l8c4UYrB6PtLUyMpQdvrbh0cT";
    private String consumerSecret = "Rxu22SYYgieQrDXUvGFPwsocFZpG3Vy4bO1SQSZTQQHCjHxV0k";
    private String token = "3300408848-6iLaGlKkPq5sR7iiaO9WY0LzRQKdrDZOyNS118d";
    private String tokenSecret = "Rngcjav4OGhCXn04T8E6yqCqyeFx6DHeyMdxo2orkMWTN";

    private TwitterProducer() {

    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run () {
        logger.info("Setup");

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        Client client = createTwitterClient(msgQueue);

        KafkaProducer<String, String> producer = createKafkaProducer();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Application");
            logger.info("Closing TwitterClient");
            client.stop();
            logger.info("CLosing Producer");
            producer.close();
            logger.info("done");

                }));
        client.connect();

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord<>("tweets_topic", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e !=null)
                        {
                            logger.error("Something went bad");
                        }
                    }
                });
            }
        }
        logger.info("Application Complete");

    }

    private KafkaProducer<String, String> createKafkaProducer() {
        String bootstrapServer = "127.0.1:9092";
        //create producer config
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Safe Producer
        producerProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProperties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        producerProperties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        producerProperties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        // HIgh THroughput at the expense of little latency and higher CPU usage

        producerProperties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        producerProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); //20 ms
        producerProperties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); //32KB

        //create producer
        KafkaProducer<String,String>  kafkaProducer = new KafkaProducer<String, String>(producerProperties);
        return kafkaProducer;
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("VeerSavarkar   ");
        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));


        Client hosebirdClient = builder.build();
        return hosebirdClient;
// Attempts to establish a connection.

    }
}
