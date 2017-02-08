package com.ai.kafka.producer.async;

import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class ASyncProduce {
	public static void main(String[] args) {
        long events = 9;
        Random rnd = new Random();
 
        Properties props = new Properties();
        props.put("metadata.broker.list", "192.168.31.128:9092,192.168.31.130:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
		//kafka.serializer.DefaultEncoder
        props.put("partitioner.class", "com.ai.kafka.producer.partition.SimplePartitioner");
		//kafka.producer.DefaultPartitioner: based on the hash of the key
        //props.put("request.required.acks", "1");
		props.put("producer.type", "sync");
		//props.put("producer.type", "1");
		// 1: sync 2: async
 
        ProducerConfig config = new ProducerConfig(props);
 
        Producer<String, String> producer = new Producer<String, String>(config);
 
        for (long nEvents = 0; nEvents < events; nEvents++) { 
               long runtime = new Date().getTime();  
               String ip = "192.168.2." + rnd.nextInt(255); 
               String msg = runtime + ",www.example.com," + ip; 
               KeyedMessage<String, String> data = new KeyedMessage<String, String>("topic2", ip, msg);
               producer.send(data);

        }
        producer.close();
    }
}
