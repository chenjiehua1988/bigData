package consumer1;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;

/**
 * Created by IntelliJ IDEA.
 * Author: cjh
 * Date: 2017/2/7
 * Time: 17:30
 * Description：该类的作用
 * To change this template use File | Settings | File Templates.
 */
public class Main {

    public static void main(String[] args)
    {

        Config config= new Config();
        config.setDebug(false);
        config.setNumWorkers(1);
        config.setMaxSpoutPending(100);
        config.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 60);

        TopologyBuilder builder= new TopologyBuilder();



        builder.setSpout("spout1", new Spout1());
        builder.setBolt("bolt1", new Bolt1(), 2).shuffleGrouping("spout1");

        LocalCluster localCluster= new LocalCluster();
        localCluster.submitTopology("topo1", config, builder.createTopology());

        try {
            StormSubmitter.submitTopology("consumer1", config, builder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        }
    }
}
