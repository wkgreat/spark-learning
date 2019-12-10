package com.imooc.spark.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.UUID;

public class KafkaProducerApp {

    public static void main(String[] args) {

        String topic = "imooc_pk_offset";

        Properties props = new Properties();
        props.put("serializer.class","kafka.serializer.StringEncoder");
        props.put("metadata.broker.list","hadoop000:9092");
        props.put("request.required.acks", "1");
        props.put("partitioner.class","kafka.producer.DefaultPartitioner");
        Producer<String,String> producer = new Producer<>(new ProducerConfig(props));

        for(int index = 0; index < 100; index++) {
            producer.send(new KeyedMessage<String, String>(topic, index+"", "慕课PK哥: " + UUID.randomUUID()));
        }

        System.out.println("Kafka生产者生产数据完毕");



    }
}
