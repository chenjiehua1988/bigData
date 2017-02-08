package consumer1;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * Created by IntelliJ IDEA.
 * Author: cjh
 * Date: 2017/2/7
 * Time: 17:24
 * Description：该类的作用
 * To change this template use File | Settings | File Templates.
 */
public class Bolt1 extends BaseBasicBolt{

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        String msg= tuple.getString(0);
        System.out.println("bolt1,"+ Thread.currentThread().getName()+ msg);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("msg"));
    }
}
