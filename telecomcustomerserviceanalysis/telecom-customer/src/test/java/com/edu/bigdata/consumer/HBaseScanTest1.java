package com.edu.bigdata.consumer;

import com.edu.bigdata.consumer.util.HBaseFilterUtil;
import com.edu.bigdata.consumer.util.PropertiesUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class HBaseScanTest1 {

    private static Configuration conf = null;

    private Connection conn;

    private HTable hTable;

    static {
        conf = HBaseConfiguration.create();
    }

    @Test
    public void scanTest() throws IOException {

        // 实例化 Connection 对象
        conn = ConnectionFactory.createConnection(conf);
        // 实例化表对象（注意：此时必须是 HTable）
        hTable = (HTable) conn.getTable(TableName.valueOf(PropertiesUtil.getProperty("hbase.calllog.tableName")));

        Scan scan = new Scan();

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String startTimePoint = null;
        String endTimePoint = null;
        try {
            startTimePoint = String.valueOf(simpleDateFormat.parse("2017-01-1").getTime());
            endTimePoint = String.valueOf(simpleDateFormat.parse("2017-03-01").getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }

        Filter filter1 = HBaseFilterUtil.gteqFilter("f1", "date_time_ts", Bytes.toBytes(startTimePoint));
        Filter filter2 = HBaseFilterUtil.ltFilter("f1", "date_time_ts", Bytes.toBytes(endTimePoint));
        Filter filterList = HBaseFilterUtil.andFilter(filter1, filter2);
        scan.setFilter(filterList);

        ResultScanner resultScanner = hTable.getScanner(scan);
        // 每一个 rowkey 对应一个 result
        for (Result result : resultScanner) {
            // 每一个 rowkey 里面包含多个 cell
            Cell[] cells = result.rawCells();
            for (Cell c : cells) {
                // System.out.println("行：" + Bytes.toString(CellUtil.cloneRow(c)));
                // System.out.println("列族：" + Bytes.toString(CellUtil.cloneFamily(c)));
                // System.out.println("列：" + Bytes.toString(CellUtil.cloneQualifier(c)));
                // System.out.println("值：" + Bytes.toString(CellUtil.cloneValue(c)));
                System.out.println(Bytes.toString(CellUtil.cloneRow(c))
                        + ","
                        + Bytes.toString(CellUtil.cloneFamily(c))
                        + ":"
                        + Bytes.toString(CellUtil.cloneQualifier(c))
                        + ","
                        + Bytes.toString(CellUtil.cloneValue(c)));
            }
        }
    }
}