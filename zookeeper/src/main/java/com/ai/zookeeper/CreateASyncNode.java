package com.ai.zookeeper;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * Author: cjh
 * Date: 2016/12/27
 * Time: 10:19
 * Description：异步创建节点
 * To change this template use File | Settings | File Templates.
 */
public class CreateASyncNode implements Watcher{

    private static ZooKeeper zooKeeper;

    private static final Logger log= Logger.getLogger(CreateASyncNode.class);

    public static void main(String[] args) throws IOException, InterruptedException {

        zooKeeper= new ZooKeeper("192.168.31.131:2181", 5000, new CreateASyncNode());

        log.info(zooKeeper.getState());

        Thread.sleep(Integer.MAX_VALUE);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {

        log.info("收到事件："+ watchedEvent);
        if (watchedEvent.getState().equals(Event.KeeperState.SyncConnected)) {
            doSomething();
        }
    }

    private void doSomething(){

        zooKeeper.create("/node2/node2_1", "111".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new IStringCallback(), "创建");
    }

    static class IStringCallback implements AsyncCallback.StringCallback
    {

        @Override
        public void processResult(int i, String s, Object o, String s1) {

            StringBuilder stringBuilder= new StringBuilder();
            stringBuilder.append("i="+ i).append("\ns="+s).append("\no="+ o+ "\ns1="+ s1);
            log.info(stringBuilder.toString());
        }
    }
}
