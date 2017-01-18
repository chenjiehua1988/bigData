package com.ai.kafka.consumer.group;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by IntelliJ IDEA.
 * Author: cjh
 * Date: 2017/1/12
 * Time: 15:39
 * Description：组消费模型
 * To change this template use File | Settings | File Templates.
 */
public class ConsumerTest extends Thread{

    private final ConsumerConnector consumerConnector;
    private final String topic;
    private ExecutorService executorService;
    private static final Logger log= Logger.getLogger(ConsumerTest.class);

    public ConsumerTest(String a_zookeeper, String a_groupId, String a_topic) {

        consumerConnector= kafka.consumer.Consumer.createJavaConsumerConnector(getConsumerConfig(a_zookeeper, a_groupId));
        this.topic = a_topic;
    }

    public void shutdown()
    {
        if (consumerConnector!= null)
        {
            consumerConnector.shutdown();
        }

        if (executorService!= null)
        {
            executorService.shutdown();
        }

        try {
            if (!executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS))
            {
                log.error("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            log.error("Interrupted during shutdown, exiting uncleanly", e);
        }
    }

    public static ConsumerConfig getConsumerConfig(String a_zookeeper, String a_groupId)
    {
        Properties props= new Properties();

        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "2000");
        props.put("auto.commit.interval.ms", "1000");
        props.put("rebalance.backoff.ms", "2000");
        props.put("rebalance.max.retries", "10");

        return new ConsumerConfig(props);
    }

    public void run(int num) {

        Map<String, Integer> map= new HashMap<String, Integer>();
        map.put(topic, num);
        Map<String, List<KafkaStream<byte[], byte[]>>> map1= consumerConnector.createMessageStreams(map);
        List<KafkaStream<byte[], byte[]>> list= map1.get(topic);

        log.info("size= "+ list.size());
        executorService= Executors.newFixedThreadPool(num);
        for (int i = 0; i < list.size(); i++) {
            KafkaStream<byte[], byte[]> messageAndMetadatas = list.get(i);

            executorService.submit(new Consumer(messageAndMetadatas, i));
        }
    }

    public static void main(String[] args)
    {

        String zookeeper= "192.168.31.128:2181,192.168.31.130:2181,192.168.31.131:2181";
        String groupId= "group1";
        String topic= "topic2";

        int threads= 1;

        ConsumerTest consumerTest= new ConsumerTest(zookeeper, groupId, topic);
        consumerTest.run(threads);

        try {
            Thread.sleep(1000* 60* 30);
        } catch (InterruptedException e) {

            log.error(e);
        }

        consumerTest.shutdown();
    }

    static class Consumer implements Runnable {
        private KafkaStream m_stream;
        private int m_threadNumber;

        public Consumer(KafkaStream a_stream, int a_threadNumber) {
            m_threadNumber = a_threadNumber;
            m_stream = a_stream;
        }

        public void run() {
            ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
            while (it.hasNext()){
                System.out.println("Thread " + m_threadNumber + ": " + new String(it.next().message()));

            }
            log.info("Shutting down Thread: " + m_threadNumber);
        }
    }
}
