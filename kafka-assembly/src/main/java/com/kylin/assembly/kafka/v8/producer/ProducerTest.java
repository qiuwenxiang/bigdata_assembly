package com.kylin.assembly.kafka.v8.producer;


import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @description: 0.8版本生产者
 * @author: kylin.qiuwx@foxmail.com
 * @create: 2018-05-04
 **/
public class ProducerTest {
    public static void main(String[] args) {
        Properties props = new Properties();

        String brokers = "demo169.test.com:6667,demo167.test.com:6667,demo168.test.com:6667";

        props.put("metadata.broker.list", brokers);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");









    }
}
