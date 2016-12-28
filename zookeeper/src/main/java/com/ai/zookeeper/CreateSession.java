package com.ai.zookeeper;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * Author: cjh
 * Date: 2016/12/27
 * Time: 10:19
 * Description：新建一个客户端连接
 * To change this template use File | Settings | File Templates.
 */
public class CreateSession implements Watcher{

    private static ZooKeeper zooKeeper;

    private static final Logger log= Logger.getLogger(CreateSession.class);

    public static void main(String[] args) throws IOException, InterruptedException {

        zooKeeper= new ZooKeeper("192.168.31.131:2181", 5000, new CreateSession());

        log.info(zooKeeper.getState());

        Thread.sleep(Integer.MAX_VALUE);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {

        log.info("收到事件："+ watchedEvent);
        if (watchedEvent.getState().equals(Event.KeeperState.SyncConnected))
        {
            doSomething();
        }
    }

    private void doSomething() {

        log.info("do Something");
    }
}
