package com.ai.zookeeper;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * Author: cjh
 * Date: 2016/12/27
 * Time: 10:19
 * Description：同步创建节点
 * To change this template use File | Settings | File Templates.
 */
public class CreateSyncNode implements Watcher{

    private static ZooKeeper zooKeeper;

    private static final Logger log= Logger.getLogger(CreateSyncNode.class);

    public static void main(String[] args) throws IOException, InterruptedException {

        zooKeeper= new ZooKeeper("192.168.31.131:2181", 5000, new CreateSyncNode());

        log.info(zooKeeper.getState());

        Thread.sleep(Integer.MAX_VALUE);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {

        log.info("收到事件："+ watchedEvent);
        if (watchedEvent.getState().equals(Event.KeeperState.SyncConnected))
        {
            try {
                doSomething();
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void doSomething() throws KeeperException, InterruptedException {

        String path= zooKeeper.create("/node2", "111".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        log.info("return created path:"+ path);
    }
}
