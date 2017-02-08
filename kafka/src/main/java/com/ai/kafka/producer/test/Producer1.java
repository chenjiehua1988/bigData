package com.ai.kafka.producer.test;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.Random;

/**
 * Created by IntelliJ IDEA.
 * Author: cjh
 * Date: 2017/2/7
 * Time: 15:15
 * Description：该类的作用
 * To change this template use File | Settings | File Templates.
 */
public class Producer1 {

    private static final Logger log= Logger.getLogger(Producer1.class);

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("metadata.broker.list", "192.168.31.128:9092,192.168.31.130:9092,192.168.31.131:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //kafka.serializer.DefaultEncoder
        props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
        //kafka.producer.DefaultPartitioner: based on the hash of the key
        props.put("request.required.acks", "1");
        //0;  绝不等确认  1:   leader的一个副本收到这条消息，并发回确认 -1：   leader的所有副本都收到这条消息，并发回确认

        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(config);

        long size= 600;
        long count= 0;
        while(true)
        {
            for (long nEvents = 0; nEvents < size; nEvents++) {
                long runtime =System.currentTimeMillis();

                String msg = runtime+ ",当前数字="+ count*600+ nEvents;
                //eventKey必须有（即使自己的分区算法不会用到这个key，也不能设为null或者""）,否者自己的分区算法根本得不到调用
                KeyedMessage<String, String> data = new KeyedMessage<String, String>("topic5", msg, msg);
                //			 eventTopic, eventKey, eventBody
                producer.send(data);
            }

            count++;
            try {
                log.info("等待1分钟之后再次发送。。。");
                Thread.sleep(1000* 60);
            } catch (InterruptedException e) {

                log.error(e);
            }
        }
    }
}
