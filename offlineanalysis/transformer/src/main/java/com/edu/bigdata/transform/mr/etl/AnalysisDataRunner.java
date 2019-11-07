package com.edu.bigdata.transform.mr.etl;

import com.edu.bigdata.transform.common.EventLogConstants;
import com.edu.bigdata.transform.common.GlobalConstants;
import com.edu.bigdata.transform.util.TimeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;


public class AnalysisDataRunner implements Tool {

    private Configuration conf = null;

    public static void main(String[] args) {
        try {
            int resultCode = ToolRunner.run(new AnalysisDataRunner(), args);
            if (resultCode == 0) {
                System.out.println("Success!");
            } else {
                System.out.println("Fail!");
            }
            System.exit(resultCode);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public Configuration getConf() {
        // 全局的访问方法
        return this.conf;
    }

    @Override
    public void setConf(Configuration conf) {
        // 先实例化 Configuration
        this.conf = HBaseConfiguration.create(conf);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        // 处理传入的时间参数，默认或不合法时间则直接使用昨天日期
        this.processArgs(conf, args);

        // 开始创建 Job
        Job job = Job.getInstance(conf, "Event_ETL");

        // 设置 Job 参数
        job.setJarByClass(AnalysisDataRunner.class);

        // Mapper 参数设置
        job.setMapperClass(AnalysisDataMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Put.class);

        // Reducer 参数设置
        job.setNumReduceTasks(0);

        // 设置数据输入路径
        initJobInputPath(job);

        // 设置输出到 HBase 的信息
        initHBaseOutPutConfig(job);
        // job.setJar("target/transformer-0.0.1-SNAPSHOT.jar");  // 如果在 Linux 上的 Eclipse 运行代码，则需要该配置

        // Job 提交
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 初始化 Job 数据输入目录
     *
     * @param job
     * @throws IOException
     */
    private void initJobInputPath(Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        // 获取要执行ETL操作的那一天的数据
        String date = conf.get(GlobalConstants.RUNNING_DATE_PARAMES); // 2017-08-14
        // 格式化 HDFS 文件路径
        String hdfsPath = TimeUtil.parseLong2String(TimeUtil.parseString2Long(date), "yyyy/MM/dd");// 2017/08/14
        if (GlobalConstants.HDFS_LOGS_PATH_PREFIX.endsWith("/")) {
            hdfsPath = GlobalConstants.HDFS_LOGS_PATH_PREFIX + hdfsPath; // /event_logs/2017/08/14
        } else {
            hdfsPath = GlobalConstants.HDFS_LOGS_PATH_PREFIX + File.separator + hdfsPath; // /event_logs/2017/08/14
            // File.separator 的作用是：根据当前操作系统获取对应的文件分隔符，windows中是 \ ，Linux中是 /
        }

        FileSystem fs = FileSystem.get(conf);
        Path inPath = new Path(hdfsPath);

        if (fs.exists(inPath)) {
            FileInputFormat.addInputPath(job, inPath);
        } else {
            throw new RuntimeException("HDFS 中该文件目录不存在：" + hdfsPath);
        }
    }

    /**
     * 设置输出到 HBase 的一些操作选项
     *
     * @throws IOException
     */
    private void initHBaseOutPutConfig(Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        // 获取要执行ETL操作的那一天的数据
        String date = conf.get(GlobalConstants.RUNNING_DATE_PARAMES); // 2017-08-14
        // 格式化 HBase 表的后缀名
        String tableNameSuffix = TimeUtil.parseLong2String(TimeUtil.parseString2Long(date), TimeUtil.HBASE_TABLE_NAME_SUFFIX_FORMAT); // 20170814
        // 构建表名
        String tableName = EventLogConstants.HBASE_NAME_EVENT_LOGS + tableNameSuffix; // event_logs20170814

        // 指定输出（初始化 ReducerJob）
        TableMapReduceUtil.initTableReducerJob(tableName, null, job);

        Connection conn = null;
        Admin admin = null;

        // 使用 HBase 的新 API
        conn = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();

        // 创建表描述器（即通过表名实例化表描述器）
        TableName tn = TableName.valueOf(tableName);
        HTableDescriptor htd = new HTableDescriptor(tn);

        // 设置列族
        htd.addFamily(new HColumnDescriptor(EventLogConstants.EVENT_LOGS_FAMILY_NAME));
        // 判断表是否存在
        if (admin.tableExists(tn)) {
            // 存在，则删除
            if (admin.isTableEnabled(tn)) {
                // 先将表设置为不可用
                admin.disableTable(tn);
            }
            // 再删除表
            admin.deleteTable(tn);
        }

        // 创建表，在创建的过程中可以考虑预分区操作
        // 假设预分区为 3个分区
        // byte[][] keySplits = new byte[3][];
        // keySplits[0] = Bytes.toBytes("1"); // (-∞, 1]
        // keySplits[1] = Bytes.toBytes("2"); // (1, 2]
        // keySplits[2] = Bytes.toBytes("3"); // (2, ∞]
        // admin.createTable(htd, keySplits);

        admin.createTable(htd);
        admin.close();
    }

    /**
     * 处理时间参数，如果没有传递参数的话，则默认清洗前一天的。
     * <p>
     * Job脚本如下： bin/yarn jar ETL.jar com.z.transformer.mr.etl.AnalysisDataRunner -date 2017-08-14
     *
     * @param args
     */
    private void processArgs(Configuration conf, String[] args) {
        String date = null;
        for (int i = 0; i < args.length; i++) {
            if ("-date".equals(args[i])) { // 找到 "-date" 标记
                if (i + 1 < args.length) {
                    date = args[i + 1]; // 获取时间
                    break;
                }
            }
        }

        if (StringUtils.isBlank(date) || !TimeUtil.isValidateRunningDate(date)) {
            // 如果没有传递参数，默认清洗昨天的数据然后存储到 HBase 中
            date = TimeUtil.getYesterday();
        }
        // 将要清洗的目标时间字符串保存到 conf 对象中（这样全局中就可以引用）
        conf.set(GlobalConstants.RUNNING_DATE_PARAMES, date);
    }
}
