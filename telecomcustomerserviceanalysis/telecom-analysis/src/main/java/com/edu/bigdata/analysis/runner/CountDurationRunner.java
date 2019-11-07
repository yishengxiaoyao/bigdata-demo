package com.edu.bigdata.analysis.runner;

import com.edu.bigdata.analysis.kv.key.ComDimension;
import com.edu.bigdata.analysis.kv.value.CountDurationValue;
import com.edu.bigdata.analysis.mapper.CountDurationMapper;
import com.edu.bigdata.analysis.outputformat.MySQLOutputFormat;
import com.edu.bigdata.analysis.reducer.CountDurationReducer;
import com.edu.bigdata.analysis.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class CountDurationRunner implements Tool {

    private Configuration conf = null;

    @Override
    public void setConf(Configuration conf) {           // conf默认是从resources中加载，加载文件的顺序是：
        this.conf = HBaseConfiguration.create(conf);    // core-default.xml -> core-site.xml -> hdfs-default.xml -> hdfs-site.xml -> hbase-default.xml -> hbase-site.xml
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public int run(String[] strings) throws Exception {
        // 得到conf
        // 实例化Job
        Job job = Job.getInstance(conf, "CALLLOG_ANALYSIS");
        job.setJarByClass(CountDurationRunner.class);

        // 组装Mapper Inputformat（注意：Inputformat 需要使用 HBase 提供的 HBaseInputformat 或者使用自定义的 Inputformat）
        initHBaseInputConfig(job);

        // 组装Reducer Outputformat
        initReducerOutputConfig(job);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private void initHBaseInputConfig(Job job) {

        Connection conn = null;
        Admin admin = null;

        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();

            if (!admin.tableExists(TableName.valueOf(Constants.SCAN_TABLE_NAME))) {
                throw new RuntimeException("无法找到目标表");
            }

            Scan scan = new Scan();
            // 可以对Scan进行优化
            // scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(Constants.SCAN_TABLE_NAME));

            TableMapReduceUtil.initTableMapperJob(
                    Constants.SCAN_TABLE_NAME,  // 数据源的表名
                    scan,                       // scan扫描控制器
                    CountDurationMapper.class,  // 设置Mapper类
                    ComDimension.class,         // 设置Mapper输出key类型
                    Text.class,                 // 设置Mapper输出value值类型
                    job,                        // 设置给哪个Job
                    true
            );
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (admin != null) {
                    admin.close();
                }
                if (conn != null && conn.isClosed()) {
                    conn.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void initReducerOutputConfig(Job job) {
        job.setReducerClass(CountDurationReducer.class);

        job.setOutputKeyClass(ComDimension.class);
        job.setOutputValueClass(CountDurationValue.class);

        job.setOutputFormatClass(MySQLOutputFormat.class);
    }

    public static void main(String[] args) {
        try {
            int status = ToolRunner.run(new CountDurationRunner(), args);
            System.exit(status);
            if (status == 0) {
                System.out.println("运行成功");
            } else {
                System.out.println("运行失败");
            }
        } catch (Exception e) {
            System.out.println("运行失败");
            e.printStackTrace();
        }
    }
}