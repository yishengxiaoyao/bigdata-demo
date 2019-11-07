package com.edu.bigdata.consumer.observer;

import com.edu.bigdata.consumer.util.HBaseUtil;
import com.edu.bigdata.consumer.util.PropertiesUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * 在协处理器中，一条主叫日志成功插入后，将该日志切换为被叫视角再次插入一次，放入到与主叫日志不同的列族中。
 */
public class CalleeWriteObserver extends BaseRegionObserver {

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
            throws IOException {
        super.postPut(e, put, edit, durability);

        // 1、获取你想要操作的目标表的名称
        String targetTableName = PropertiesUtil.getProperty("hbase.calllog.tableName");

        // 2、获取当前操作的表的表名
        String currentTableName = e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();

        // 3、判断需要操作的表是否就是当前表，如果不是，则直接 return
        if (!targetTableName.equals(currentTableName)) {
            return;
        }

        // 4、得到当前插入数据的值并封装新的数据，
        // oriRowkey举例：01_15369468720_20170727081033_13720860202_1_0180
        String oriRowkey = Bytes.toString(put.getRow());

        String[] splits = oriRowkey.split("_");

        // 如果当前插入的是被叫数据，则直接返回(因为默认提供的数据全部为主叫数据)，
        // 又因为我们使用的协处理器的方法是postPut()，即每次插入一条主叫数据后，都会调用该方法插入一条被叫数据。
        // 插入一条被叫数据后，又会继续调用该方法，此时插入的数据是被叫数据，需要及时停掉，否则会形成死循环！
        String oldFlag = splits[4];
        if (oldFlag.equals("0")) {
            return;
        }

        // 组装新的数据所在分区号
        int regions = Integer.valueOf(PropertiesUtil.getProperty("hbase.calllog.regions.count"));

        String call1 = splits[1];
        String call2 = splits[3];
        String buildTime = splits[2];
        String duration = splits[5];

        String newFlag = "0";

        // 生成时间戳
        String buildTime_ts = null;
        try {
            buildTime_ts = String.valueOf(new SimpleDateFormat("yyyyMMddHHmmss").parse(buildTime).getTime());
        } catch (ParseException e1) {
            e1.printStackTrace();
        }

        // 生成新的分区号
        String regionCode = HBaseUtil.genRegionCode(call2, buildTime, regions);

        // 拼接数据，生成新的 RowKey
        // 新 RowKey 形式：03_13720860202_20170727081033_15369468720_0_0180
        String rowKey = HBaseUtil.genRowKey(regionCode, call2, buildTime, call1, newFlag, duration);

        // 向 HBase 表中插入数据（被叫）
        Put calleePut = new Put(Bytes.toBytes(rowKey));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("call1"), Bytes.toBytes(call2));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("call2"), Bytes.toBytes(call1));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("build_time"), Bytes.toBytes(buildTime));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("build_time_ts"), Bytes.toBytes(buildTime_ts));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("flag"), Bytes.toBytes(newFlag));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("duration"), Bytes.toBytes(duration));

        Table table = e.getEnvironment().getTable(TableName.valueOf(targetTableName));
        table.put(calleePut);

        table.close();
    }
}