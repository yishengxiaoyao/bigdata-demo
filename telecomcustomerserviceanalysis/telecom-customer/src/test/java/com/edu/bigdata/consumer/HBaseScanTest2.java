package com.edu.bigdata.consumer;

import com.edu.bigdata.consumer.util.PropertiesUtil;
import com.edu.bigdata.consumer.util.ScanRowkeyUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;

public class HBaseScanTest2 {

    private static Configuration conf = null;

    private Connection conn;

    private HTable hTable;

    static {
        conf = HBaseConfiguration.create();
    }

    @Test
    public void scanTest() throws IOException, ParseException {

        String call = "14473548449";
        String startPoint = "2017-01-01";
        String stopPoint = "2017-09-01";

        // 实例化 Connection 对象
        conn = ConnectionFactory.createConnection(conf);
        // 实例化表对象（注意：此时必须是 HTable）
        hTable = (HTable) conn.getTable(TableName.valueOf(PropertiesUtil.getProperty("hbase.calllog.tableName")));

        Scan scan = new Scan();
        ScanRowkeyUtil scanRowkeyUtil = new ScanRowkeyUtil(call, startPoint, stopPoint);
        while (scanRowkeyUtil.hasNext()) {

            String[] rowKeys = scanRowkeyUtil.next();
            scan.setStartRow(Bytes.toBytes(rowKeys[0]));
            scan.setStopRow(Bytes.toBytes(rowKeys[1]));

            System.out.println("时间范围" + rowKeys[0].substring(15, 21) + "---" + rowKeys[1].substring(15, 21));

            ResultScanner resultScanner = hTable.getScanner(scan);
            // 每一个 rowkey 对应一个 result
            for (Result result : resultScanner) {
                // 每一个 rowkey 里面包含多个 cell
                Cell[] cells = result.rawCells();
                StringBuilder sb = new StringBuilder();
                sb.append(Bytes.toString(result.getRow())).append(",");

                for (Cell c : cells) {
                    sb.append(Bytes.toString(CellUtil.cloneValue(c))).append(",");
                }
                System.out.println(sb.toString());
            }
        }
    }
}