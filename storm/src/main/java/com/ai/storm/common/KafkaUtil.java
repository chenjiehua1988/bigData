package com.ai.storm.common;


import backtype.storm.Config;
import storm.kafka.DynamicBrokersReader;

import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * Author: cjh
 * Date: 2017/1/18
 * Time: 15:46
 * Description：该类的作用
 * To change this template use File | Settings | File Templates.
 */
public class KafkaUtil {

    public static void printKafkaBrokers() throws SocketTimeoutException {
        // Map conf, String zkStr, String zkPath, String topic
        Map<String, Integer> conf = new HashMap<String, Integer>();
        String zkStr = "192.168.31.128:2181,192.168.31.130:2181,192.168.31.131:2181";
        String zkPath = "/brokers";
        String topic = "topic5";

        conf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 100000);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, 10);
        conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, 1000);
        conf.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, 100000);

        DynamicBrokersReader readFromKafKa = new DynamicBrokersReader(conf, zkStr, zkPath, topic);
        System.out.println("Kafka broker list--------->" + readFromKafKa.getBrokerInfo());
    }

    public static void main(String[] args)
    {

        try {
            printKafkaBrokers();
        } catch (SocketTimeoutException e) {
            e.printStackTrace();
        }
    }
}
