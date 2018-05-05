package com.kylin.assembly.kafka.v9.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @description: 0.9版本生产者
 * @author: kylin.qiuwx@foxmail.com
 * @create: 2018-05-04
 **/
public class ProducerTest {
    public static void main(String[] args) {
        Properties props = new Properties();

        String brokers = "demo169.test.com:6667,demo167.test.com:6667,demo168.test.com:6667";

        props.put("bootstrap.servers", brokers);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++){
            producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
            System.out.println("===V9==");

        }

        producer.close();
    }
}
