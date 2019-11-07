package com.edu.spark.zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZookeeperCreateApp {
    private static Logger LOGGER = LoggerFactory.getLogger(ZookeeperCreateApp.class);
    private static CountDownLatch connected = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {
        ZooKeeper zooKeeper = new ZooKeeper("localhost:2181", 5000, new ZookeeperWatcher(connected));
        String path = "/zk_xiaoyao";
        String data = "";
        boolean needWatcher = false;
        System.out.println(zooKeeper.getState());
        connected.await();
        System.out.println(zooKeeper.getState());
        //getChildern(zooKeeper,path);
        //createNode(zooKeeper,path,data);
        //setData(zooKeeper,path);
        //getData(zooKeeper,path,needWatcher);
        deleteNode(zooKeeper, path);
    }

    public static void getChildern(ZooKeeper zooKeeper, String path) throws KeeperException, InterruptedException {
        List<String> result = zooKeeper.getChildren(path, true);
        for (String temp : result) {
            System.out.println(temp);
        }
    }

    public static void createNode(ZooKeeper zooKeeper, String path, String data) throws InterruptedException, KeeperException {
        zooKeeper.exists(path, true);
        zooKeeper.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("create node successfully!");
    }

    public static void setData(ZooKeeper zooKeeper, String path) throws InterruptedException, KeeperException {
        Stat stat = zooKeeper.setData(path, "xiaoyaosan".getBytes(), -1);
        System.out.println(stat.getVersion());
    }

    public static void getData(ZooKeeper zooKeeper, String path, boolean needWatcher) throws InterruptedException, KeeperException {
        String result = new String(zooKeeper.getData(path, needWatcher, null));
        System.out.println(result);
    }

    public static void deleteNode(ZooKeeper zooKeeper, String path) throws InterruptedException, KeeperException {
        zooKeeper.delete(path, 1);
    }
}
