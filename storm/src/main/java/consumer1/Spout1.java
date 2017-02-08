package consumer1;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
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

/**
 * Created by IntelliJ IDEA.
 * Author: cjh
 * Date: 2017/2/8
 * Time: 9:55
 * Description：该类的作用
 * To change this template use File | Settings | File Templates.
 */
public class Spout1 extends BaseRichSpout {

    private SpoutOutputCollector collector;

    private ConsumerConnector consumerConnector;
    private String topic;
    private String zk;
    private String groupId;
    private ExecutorService executorService;
    private static final Logger log= Logger.getLogger(Spout1.class);


    public ConsumerConfig getConsumerConfig()
    {
        Properties props= new Properties();

        props.put("zookeeper.connect", zk);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.async.time.ms", "2000");
        props.put("auto.commit.interval.ms", "1000");
        props.put("rebalance.backoff.ms", "2000");
        props.put("rebalance.max.retries", "10");

        return new ConsumerConfig(props);
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        String zk= "192.168.31.128:2181,192.168.31.130:2181,192.168.31.131:2181";
        String topic= "topic5";
        String groupId= "groupId";

        this.topic = topic;
        this.zk= zk;
        this.groupId= groupId;
        this.consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(getConsumerConfig());

        this.collector= spoutOutputCollector;
    }

    @Override
    public void nextTuple() {

        System.out.println("--------------初始化spout-----------------");

        int threads= 2;

        Map<String, Integer> map= new HashMap<String, Integer>();
        map.put(topic, threads);
        Map<String, List<KafkaStream<byte[], byte[]>>> map1= consumerConnector.createMessageStreams(map);
        List<KafkaStream<byte[], byte[]>> list= map1.get(topic);

        executorService= Executors.newFixedThreadPool(threads);
        for (int i = 0; i < list.size(); i++) {
            KafkaStream<byte[], byte[]> messageAndMetadatas = list.get(i);

            executorService.submit(new Consumer(messageAndMetadatas, i, collector));
        }

        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            log.error(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("msg"));
    }

    static class Consumer implements Runnable {
        private KafkaStream m_stream;
        private int m_threadNumber;
        private SpoutOutputCollector collector;

        public Consumer(KafkaStream a_stream, int a_threadNumber, SpoutOutputCollector collector) {
            m_threadNumber = a_threadNumber;
            m_stream = a_stream;
            this.collector= collector;
        }

        public void run() {
            ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
            while (it.hasNext()){
//                System.out.println("Thread " + m_threadNumber + ": " + new String(it.next().message()));

                collector.emit(new Values(new String(it.next().message())));
            }
            log.info("Shutting down Thread: " + m_threadNumber);
        }
    }
}
