package com.edu.spark.zk;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.List;

public class CuratorZookeeperApp {
    private static final String SERVER = "localhost:2181";
    private final int SESSION_TIMEOUT = 30 * 1000;
    private final int CONNECT_TIMEOUT = 3 * 1000;
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    private CuratorFramework client = null;

    @Before
    public void init() {
        client = CuratorFrameworkFactory.newClient(SERVER, SESSION_TIMEOUT, CONNECT_TIMEOUT, retryPolicy);
        client = CuratorFrameworkFactory.builder().connectString("localhost:2181")
                .sessionTimeoutMs(20000).retryPolicy(retryPolicy)
                .namespace("g5").build();
        client.start();
    }

    @Test
    public void testCreate() throws Exception {
        //创建永久节点
        //client.create().forPath("/curator");
        //创建永久有序节点
        //client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath("/curator_sequential");
        //创建临时节点
        //client.create().withMode(CreateMode.EPHEMERAL).forPath("/curator/ephemeral");
        //创建临时有序节点
        //client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/curator/ephemeral_sequence");

        //client.create().withProtection().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/curator/ephemeral_sequence_protect");

    }

    @Test
    public void testExists() throws Exception {
        Stat stat = client.checkExists().forPath("/curator");
        System.out.println("stat exists:" + (stat != null));
        Stat stattemp = client.checkExists().forPath("/curator_sequential0000000002");
        System.out.println("stattemp exists:" + (stattemp != null));
        Stat stat2 = client.checkExists().forPath("/curator35");
        System.out.println("stat2 exists:" + (stat2 != null));
    }

    @Test
    public void testGetChildren() throws Exception {
        //获取当前节点的所有子节点
        List<String> nodeList = client.getChildren().forPath("/");
        System.out.println(nodeList);
        //获取节点的数据
        List<String> node = client.getChildren().forPath("/curator");
        System.out.println(node);
    }

    @Test
    public void testGetAndSetData() throws Exception {
        //set data
        client.setData().withVersion(0).forPath("/huahua", "huahua data".getBytes());
        //get data
        String result = new String(client.getData().forPath("/huahua"));
        System.out.println(result);
    }

    @Test
    public void testDelete() throws Exception {
        //删除该节点
        client.delete().forPath("/huahua");
        //删除级联节点
        client.delete().guaranteed().deletingChildrenIfNeeded().forPath("/huahua");
    }

    @Test
    public void testNamespace() throws Exception {
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .namespace("xiaoyao")
                .connectString("localhost:2181")
                .sessionTimeoutMs(SESSION_TIMEOUT)
                .connectionTimeoutMs(CONNECT_TIMEOUT)
                .retryPolicy(retryPolicy)
                .build();
        //启动
        curatorFramework.start();
        curatorFramework.create().creatingParentsIfNeeded().forPath("/server/method", "servermethod".getBytes());
        curatorFramework.close();
    }

    /**
     * 测试异步设置节点数据
     *
     * @throws Exception
     */
    @Test
    public void testSetDataAsync() throws Exception {
        //创建监听器
        CuratorListener listener = new CuratorListener() {

            public void eventReceived(CuratorFramework client, CuratorEvent event)
                    throws Exception {
                System.out.println(event.getPath());
            }
        };

        //添加监听器
        client.getCuratorListenable().addListener(listener);

        //异步设置某个节点数据
        client.setData().inBackground().forPath("/curator", "/curator modified data with Async".getBytes());

        //为了防止单元测试结束从而看不到异步执行结果，因此暂停10秒
        Thread.sleep(10000);
    }

    /**
     * 测试另一种异步执行获取通知的方式
     *
     * @throws Exception
     */
    @Test
    public void testSetDataAsyncWithCallback() throws Exception {
        BackgroundCallback callback = new BackgroundCallback() {


            public void processResult(CuratorFramework client, CuratorEvent event)
                    throws Exception {
                System.out.println(event.getPath());
            }
        };

        //异步设置某个节点数据
        client.setData().inBackground(callback).forPath("/curator", "/curator modified data with Callback".getBytes());

        //为了防止单元测试结束从而看不到异步执行结果，因此暂停10秒
        Thread.sleep(10000);
    }

    /**
     * 测试事务管理：碰到异常，事务会回滚
     *
     * @throws Exception
     */
    @Test
    public void testTransaction() throws Exception {
        //定义几个基本操作

        Collection<CuratorTransactionResult> results = client.inTransaction().create().forPath("/curator/one_path", "some data".getBytes())
                .and().setData().forPath("/curator", "other data".getBytes())
                .and().delete().forPath("/curator")
                .and().commit();

        //遍历输出结果
        for (CuratorTransactionResult result : results) {
            System.out.println("执行结果是： " + result.getForPath() + "--" + result.getType());
        }
    }

    @After
    public void close() {
        client.close();
    }

}
