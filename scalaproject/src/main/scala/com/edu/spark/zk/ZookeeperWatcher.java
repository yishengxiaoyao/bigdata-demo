package com.edu.spark.zk;


import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.concurrent.CountDownLatch;

public class ZookeeperWatcher implements Watcher {

    private CountDownLatch connected;

    public ZookeeperWatcher() {
    }

    public ZookeeperWatcher(CountDownLatch connected) {
        this.connected = connected;
    }

    public void process(WatchedEvent watchedEvent) {
        if (Event.KeeperState.SyncConnected == watchedEvent.getState()) {
            connected.countDown();
        }
    }

}
