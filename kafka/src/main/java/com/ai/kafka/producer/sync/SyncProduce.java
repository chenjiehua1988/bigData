package com.ai.kafka.producer.sync;
import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class SyncProduce {
	public static void main(String[] args) {
        long events = 10;
        Random rnd = new Random();
 
        Properties props = new Properties();
        props.put("metadata.broker.list", "192.168.31.128:9092,192.168.31.130:9092,192.168.31.131:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
		//kafka.serializer.DefaultEncoder
        props.put("partitioner.class", "com.ai.kafka.producer.partition.SimplePartitioner");
		//kafka.producer.DefaultPartitioner: based on the hash of the key
        props.put("request.required.acks", "1");
		//0;  绝不等确认  1:   leader的一个副本收到这条消息，并发回确认 -1：   leader的所有副本都收到这条消息，并发回确认
 
        ProducerConfig config = new ProducerConfig(props);
 
        Producer<String, String> producer = new Producer<String, String>(config);
 
        for (long nEvents = 0; nEvents < events; nEvents++) { 
               long runtime = new Date().getTime();  
               String ip = "192.168.2." + rnd.nextInt(255); 
               String msg = runtime + ",www.example.com," + ip; 
			   //eventKey必须有（即使自己的分区算法不会用到这个key，也不能设为null或者""）,否者自己的分区算法根本得不到调用
               KeyedMessage<String, String> data = new KeyedMessage<String, String>("topic1", ip, msg);
			   												//			 eventTopic, eventKey, eventBody
               producer.send(data);
			   try {
                   Thread.sleep(1000);
               } catch (InterruptedException ie) {
               }
        }
        producer.close();
    }
}
