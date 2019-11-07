package com.edu.bigdata.consumer.repository;

import com.edu.bigdata.consumer.util.HBaseUtil;
import com.edu.bigdata.consumer.util.PropertiesUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class HBaseDao {

    public static Configuration conf;

    private Connection conn;

    private Table table;

    private String namespace;

    private String tableName;

    private int regions;

    private SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");

    static {
        conf = HBaseConfiguration.create();
    }

    public HBaseDao() {
        try {

            // 获取配置文件
            namespace = PropertiesUtil.getProperty("hbase.calllog.namespace");
            tableName = PropertiesUtil.getProperty("hbase.calllog.tableName");
            regions = Integer.valueOf(PropertiesUtil.getProperty("hbase.calllog.regions.count"));

            // 实例化 Connection 对象
            conn = ConnectionFactory.createConnection(conf);
            // 实例化表对象
            table = conn.getTable(TableName.valueOf(tableName));

            if (!HBaseUtil.isExistTable(conf, tableName)) {

                HBaseUtil.initNamespace(conf, namespace);
                HBaseUtil.creatTable(conf, tableName, regions, "f1", "f2");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 将当前数据put到HTable中
     *
     * 原始数据 oriValue 形式：13231085347,18637946280,2017-06-18 20:47:26,0616
     * RowKey 形式：01_13231085347_20170618204726_18637946280_1_0616
     * HBase 表的列的形式：call1   call2   build_time  build_time_ts   flag    duration
     *
     * @param oriValue
     */
    public void put(String oriValue) {


        try {

            // 切割原始数据
            String[] splitOri = oriValue.split(",");

            // 取值赋值
            String call1 = splitOri[0];
            String call2 = splitOri[1];
            String buildTime = splitOri[2]; // 2017-06-18 20:47:26
            String duration = splitOri[3];

            // 将 2017-06-18 20:47:26 转换为 20170618204726
            String buildTimeRep = sdf2.format(sdf1.parse(buildTime));
            
            String flag = "1";

            // 生成时间戳
            String buildTime_ts = String.valueOf(sdf1.parse(buildTime).getTime());

            // 生成分区号
            String regionCode = HBaseUtil.genRegionCode(call1, buildTime, regions);

            // 拼接数据，生成 RowKey
            String rowKey = HBaseUtil.genRowKey(regionCode, call1, buildTimeRep, call2, flag, duration);

            // 向 HBase 表中插入该条数据
            Put callerPut = new Put(Bytes.toBytes(rowKey));
            callerPut.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("call1"), Bytes.toBytes(call1));
            callerPut.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("call2"), Bytes.toBytes(call2));
            callerPut.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("build_time"), Bytes.toBytes(buildTime));
            callerPut.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("build_time_ts"), Bytes.toBytes(buildTime_ts));
            callerPut.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("flag"), Bytes.toBytes(flag));
            callerPut.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("duration"), Bytes.toBytes(duration));

            table.put(callerPut);
            
            // 向 HBase 表中插入数据（被叫）
            // Put calleePut = new Put(Bytes.toBytes(rowKey));
            // ......
            // table.put(calleePut);
            // 这种方法不好，我们使用协处理器
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}