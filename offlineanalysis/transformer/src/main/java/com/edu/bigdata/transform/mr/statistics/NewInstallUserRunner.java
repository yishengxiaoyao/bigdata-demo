package com.edu.bigdata.transform.mr.statistics;

import com.edu.bigdata.transform.common.EventLogConstants;
import com.edu.bigdata.transform.common.EventLogConstants.EventEnum;
import com.edu.bigdata.transform.common.GlobalConstants;
import com.edu.bigdata.transform.dimension.key.stats.StatsUserDimension;
import com.edu.bigdata.transform.dimension.value.MapWritableValue;
import com.edu.bigdata.transform.output.TransformerMySQLOutputFormat;
import com.edu.bigdata.transform.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NewInstallUserRunner implements Tool {

    // 给定一个参数表示参数上下文
    private Configuration conf = null;

    public static void main(String[] args) {
        try {
            int exitCode = ToolRunner.run(new NewInstallUserRunner(), args);
            if (exitCode == 0) {
                System.out.println("运行成功");
            } else {
                System.out.println("运行失败");
            }
            System.exit(exitCode);
        } catch (Exception e) {
            System.err.println("执行异常:" + e.getMessage());
        }
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public void setConf(Configuration conf) {
        // 添加自己开发环境所有需要的其他资源属性文件
        conf.addResource("transformer-env.xml");
        conf.addResource("output-collector.xml");
        conf.addResource("query-mapping.xml");

        // 创建 HBase 的 Configuration 对象
        this.conf = HBaseConfiguration.create(conf);
    }

    @Override
    public int run(String[] args) throws Exception {
        // 1、获取参数上下文对象
        Configuration conf = this.getConf();

        // 2、处理传入的参数，将参数添加到上下文中
        this.processArgs(conf, args);

        // 3、创建 Job
        Job job = Job.getInstance(conf, "new_install_users");

        // 4、设置 Job 的 jar 相关信息
        job.setJarByClass(NewInstallUserRunner.class);

        // 5、设置 IntputFormat 相关配置参数
        this.setHBaseInputConfig(job);

        // 6、设置 Mapper 相关参数
        // 在 setHBaseInputConfig 已经设置了

        // 7、设置 Reducer 相关参数
        job.setReducerClass(NewInstallUserReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(MapWritableValue.class);

        // 8、设置 OutputFormat 相关参数，使用一个自定义的 OutputFormat
        job.setOutputFormatClass(TransformerMySQLOutputFormat.class);

        // 9、Job 提交运行
        boolean result = job.waitForCompletion(true);
        // 10、运行成功返回 0，失败返回 -1
        return result ? 0 : -1;
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
            if ("-date".equals(args[i])) {
                if (i + 1 < args.length) {
                    date = args[i + 1];
                    break;
                }
            }
        }
        // 查看是否需要默认参数
        if (StringUtils.isBlank(date) || !TimeUtil.isValidateRunningDate(date)) {
            date = TimeUtil.getYesterday(); // 默认时间是昨天
        }
        // 保存到上下文中间
        conf.set(GlobalConstants.RUNNING_DATE_PARAMES, date);
    }

    /**
     * 设置从 hbase 读取数据的相关配置信息
     *
     * @param job
     * @throws IOException
     */
    private void setHBaseInputConfig(Job job) throws IOException {
        Configuration conf = job.getConfiguration();

        // 获取已经执行ETL操作的那一天的数据
        String dateStr = conf.get(GlobalConstants.RUNNING_DATE_PARAMES); // 2017-08-14

        // 因为我们要访问 HBase 中的多张表，所以需要多个 Scan 对象，所以创建 Scan 集合
        List<Scan> scans = new ArrayList<Scan>();

        // 开始构建 Scan 集合
        // 1、构建 Hbase Scan Filter 对象
        FilterList filterList = new FilterList();
        // 2、构建只获取 Launch 事件的 Filter
        filterList.addFilter(new SingleColumnValueFilter(
                EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME, // 列族
                Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME), // 事件名称
                CompareOp.EQUAL, // 等于判断
                Bytes.toBytes(EventEnum.LAUNCH.alias))); // Launch 事件的别名
        // 3、构建部分列的过滤器 Filter
        String[] columns = new String[]{
                EventLogConstants.LOG_COLUMN_NAME_PLATFORM, // 平台名称
                EventLogConstants.LOG_COLUMN_NAME_VERSION, // 平台版本
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME, // 浏览器名称
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION, // 浏览器版本
                EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME, // 服务器时间
                EventLogConstants.LOG_COLUMN_NAME_UUID, // 访客唯一标识符 uuid
                EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME // 确保根据事件名称过滤数据有效，所以需要该列的值
        };

        // 创建 getColumnFilter 方法用于得到 Filter 对象
        // 根据列名称过滤数据的 Filter
        filterList.addFilter(this.getColumnFilter(columns));

        // 4、数据来源表所属日期是哪些
        long startDate, endDate; // Scan 的表区间属于[startDate, endDate)

        long date = TimeUtil.parseString2Long(dateStr); // 传入时间所属当前天开始的时间戳，即当前天的0点0分0秒的毫秒值
        long endOfDate = date + GlobalConstants.DAY_OF_MILLISECONDS; // 传入时间所属当前天结束的时间戳

        long firstDayOfWeek = TimeUtil.getFirstDayOfThisWeek(date); // 传入时间所属当前周的第一天的时间戳
        long lastDayOfWeek = TimeUtil.getFirstDayOfNextWeek(date); // 传入时间所属下一周的第一天的时间戳
        long firstDayOfMonth = TimeUtil.getFirstDayOfThisMonth(date); // 传入时间所属当前月的第一天的时间戳
        long lastDayOfMonth = TimeUtil.getFirstDayOfNextMonth(date); // 传入时间所属下一月的第一天的时间戳

        // 选择最小的时间戳作为数据输入的起始时间，date 一定大于等于其他两个 first 时间戳值

        // 获取起始时间
        startDate = Math.min(firstDayOfMonth, firstDayOfWeek);

        // 获取结束时间
        endDate = TimeUtil.getTodayInMillis() + GlobalConstants.DAY_OF_MILLISECONDS;
        if (endOfDate > lastDayOfWeek || endOfDate > lastDayOfMonth) {
            endDate = Math.max(lastDayOfMonth, lastDayOfWeek);
        } else {
            endDate = endOfDate;
        }

        // 获取连接对象，执行，这里使用 HBase 的 新 API
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = null;
        try {
            admin = connection.getAdmin();
        } catch (Exception e) {
            throw new RuntimeException("创建 Admin 对象失败", e);
        }

        // 5、构建我们 scan 集合
        try {
            for (long begin = startDate; begin < endDate; ) {
                // 格式化 HBase 的后缀
                String tableNameSuffix = TimeUtil.parseLong2String(begin, TimeUtil.HBASE_TABLE_NAME_SUFFIX_FORMAT); // 20170814
                // 构建表名称：tableName = event_logs20170814
                String tableName = EventLogConstants.HBASE_NAME_EVENT_LOGS + tableNameSuffix;

                // 需要先判断表存在，然后当表存在的情况下，再构建 Scan 对象
                if (admin.tableExists(TableName.valueOf(tableName))) {
                    // 表存在，进行 Scan 对象创建
                    Scan scan = new Scan();
                    // 需要扫描的 HBase 表名设置到 Scan 对象中
                    scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(tableName));
                    // 设置过滤对象
                    scan.setFilter(filterList);
                    // 添加到 Scan 集合中
                    scans.add(scan);
                }

                // begin 累加
                begin += GlobalConstants.DAY_OF_MILLISECONDS;
            }
        } finally {
            // 关闭 Admin 连接
            try {
                admin.close();
            } catch (Exception e) {
                // nothing
            }
        }

        // 访问 HBase 表中的数据
        if (scans.isEmpty()) {
            // 没有表存在，那么 Job 运行失败
            throw new RuntimeException("HBase 中没有对应表存在:" + dateStr);
        }


        // 指定 Mapper，注意导入的是 mapreduce 包下的，不是 mapred 包下的，后者是老版本
        TableMapReduceUtil.initTableMapperJob(
                scans, // Scan 扫描控制器集合
                NewInstallUserMapper.class, // 设置 Mapper 类
                StatsUserDimension.class,  // 设置 Mapper 输出 key 类型
                Text.class, // 设置 Mapper 输出 value 值类型
                job,  // 设置给哪个 Job
                true); // 如果在 Windows 上本地运行，则 addDependencyJars 参数必须设置为 false，如果打成 jar 包提交 Linux 上运行设置为 true，默认为 true
    }

    /**
     * 获取一个根据列名称过滤数据的 Filter
     *
     * @param columns
     * @return
     */
    private Filter getColumnFilter(String[] columns) {
        byte[][] prefixes = new byte[columns.length][];
        for (int i = 0; i < columns.length; i++) {
            prefixes[i] = Bytes.toBytes(columns[i]);
        }
        return new MultipleColumnPrefixFilter(prefixes);
    }
}