package com.edu.bigdata.consumer.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.TreeSet;

public class HBaseUtil {

    /**
     * 判断 HBase 表是否存在（使用新 HBase 的 API）
     * 小知识：当前代码块对该异常没有处理能力(业务处理能力)的时候，我们就要抛出去。
     *
     * @param conf      HBaseConfiguration
     * @param tableName
     * @return
     */
    public static boolean isExistTable(Configuration conf, String tableName) throws IOException {

        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();

        boolean result = admin.tableExists(TableName.valueOf(tableName));

        admin.close();
        conn.close();

        return result;
    }

    /**
     * 初始化命名空间
     *
     * @param conf
     * @param namespace
     */
    public static void initNamespace(Configuration conf, String namespace) throws IOException {

        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();

        // 命名空间类似于关系型数据库中的 schema，可以想象成文件夹
        // 创建命名空间描述器
        NamespaceDescriptor nd = NamespaceDescriptor
                .create(namespace)
                .addConfiguration("CREATE_TIME", String.valueOf(System.currentTimeMillis()))
                .addConfiguration("AUTHOR", "chenmingjun")
                .build();

        admin.createNamespace(nd);

        admin.close();
        conn.close();
    }

    /**
     * 创建表+预分区键
     *
     * @param conf
     * @param tableName
     * @param regions
     * @param columnFamily
     * @throws IOException
     */
    public static void creatTable(Configuration conf, String tableName, int regions, String... columnFamily) throws IOException {

        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();

        if (isExistTable(conf, tableName)) {
            System.out.println("表 " + tableName + " 已存在！");
            return;
        }

        // 创建表描述器（即通过表名实例化表描述器）
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));

        // 添加列族
        for (String cf : columnFamily) {
            // 创建列族描述器
            HColumnDescriptor hcd = new HColumnDescriptor(cf);
            // 可以设置保存的版本个数，默认是1个
            // hcd.setMaxVersions(3);
            htd.addFamily(hcd);
        }

        // 创建表操作（简单表）
        // admin.createTable(htd);

        // 为该表设置协处理器
        // htd.addCoprocessor("com.china.hbase.CalleeWriteObserver");

        // 创建表操作（加预分区）
        admin.createTable(htd, genSplitKeys(regions));

        System.out.println("表" + tableName + "创建成功！");

        admin.close();
        conn.close();
    }

    /**
     * 生成预分区键
     * 例如：{"00|", "01|", "02|", "03|", "04|", "05|"}
     *
     * @param regions
     * @return
     */
    public static byte[][] genSplitKeys(int regions) {

        // 定义一个存放预分区键的数组
        String[] keys = new String[regions];

        // 这里默认不会超过两位数的分区，如果超过，需要变更设计
        // 假设我们的 region 个数不超过两位数，所以 region 的预分区键我们格式化为两位数字所代表的字符串
        DecimalFormat df = new DecimalFormat("00");
        for (int i = 0; i < regions; i++) {
            // 例如：如果 regions = 6，则：{"00|", "01|", "02|", "03|", "04|", "05|"}
            keys[i] = df.format(i) + "|";
        }
        // 测试
        // System.out.println(Arrays.toString(keys));

        byte[][] splitKeys = new byte[regions][];

        // 生成 byte[][] 类型的预分区键的时候，一定要先保证预分区键是有序的
        TreeSet<byte[]> treeSet = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        for (int i = 0; i < regions; i++) {
            treeSet.add(Bytes.toBytes(keys[i]));
        }

        // 将排序好的预分区键放到 splitKeys 中，使用迭代器方式
        Iterator<byte[]> splitKeysIterator = treeSet.iterator();
        int index = 0;
        while (splitKeysIterator.hasNext()) {
            byte[] b = splitKeysIterator.next();
            splitKeys[index++] = b;
        }
        /*// 测试
        for (byte[] a : splitKeys) {
            System.out.println(Arrays.toString(a));
        }*/

        return splitKeys;
    }

    /**
     * 生成 RowKey
     * 形式为：regionCode_call1_buildTime_call2_flag_duration
     *
     * @param regionCode
     * @param call1
     * @param buildTime
     * @param call2
     * @param flag
     * @param duration
     * @return
     */
    public static String genRowKey(String regionCode, String call1, String buildTime, String call2, String flag, String duration) {

        StringBuilder sb = new StringBuilder();
        sb.append(regionCode + "_")
                .append(call1 + "_")
                .append(buildTime + "_")
                .append(call2 + "_")
                .append(flag + "_")
                .append(duration);

        return sb.toString();
    }

    /**
     * 生成分区号
     * 手机号：15837312345
     * 通话建立的时间：2017-01-10 11:20:30 -> 201701
     *
     * @param call1
     * @param buildTime
     * @param regions
     * @return
     */
    public static String genRegionCode(String call1, String buildTime, int regions) {

        int len = call1.length();

        // 取出手机号码后四位
        String lastPhone = call1.substring(len - 4);

        // 取出通话建立时间的年月即可，例如：201701
        String ym = buildTime.replaceAll("-", "").substring(0, 6);

        // 离散操作1
        Integer x = Integer.valueOf(lastPhone) ^ Integer.valueOf(ym);
        // 离散操作2
        int y = x.hashCode();

        // 生成分区号操作，与初始化设定的 region 个数求模
        int regionCode = y % regions;

        // 格式化分区号
        DecimalFormat df = new DecimalFormat("00");
        return df.format(regionCode);
    }

}