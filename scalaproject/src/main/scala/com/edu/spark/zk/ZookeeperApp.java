package com.edu.spark.zk;

import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class ZookeeperApp {

    private static Logger LOGGER = LoggerFactory.getLogger(ZookeeperApp.class);

    private static CountDownLatch connected = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {

        ZooKeeper zooKeeper = new ZooKeeper("localhost:2181", 5000, new ZookeeperWatcher(connected));

        System.out.println("the client connect to zk server....");

        System.out.println("link state:" + zooKeeper.getState());
        connected.await();
        System.out.println("link state:" + zooKeeper.getState());


    }

}
