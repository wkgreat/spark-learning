package com.imooc.spark.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaProducer extends Thread {

    private String topic;

    private Producer<Integer, String> producer;

    public KafkaProducer(String topic) {
        this.topic = topic;

        Properties properties = new Properties();

        properties.setProperty("metadata.broker.list",KafkaProperties.BROKER_LIST);
        properties.setProperty("serializer.class","kafka.serializer.StringEncoder");
        properties.setProperty("request.required.acks","1");

        producer = new Producer<Integer, String>(new ProducerConfig(properties));


    }

    @Override
    public void run() {

        int messageNo = 1;

        while(true) {
            String message = "message_" + messageNo;
            producer.send(new KeyedMessage<Integer, String>(topic, message));
            System.out.println("Sent: " + message);
            messageNo++;
            try {
                TimeUnit.MILLISECONDS.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }


}
