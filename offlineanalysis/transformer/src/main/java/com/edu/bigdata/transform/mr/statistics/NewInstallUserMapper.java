package com.edu.bigdata.transform.mr.statistics;

import com.edu.bigdata.transform.common.DateEnum;
import com.edu.bigdata.transform.common.EventLogConstants;
import com.edu.bigdata.transform.common.GlobalConstants;
import com.edu.bigdata.transform.common.KpiType;
import com.edu.bigdata.transform.dimension.key.base.BrowserDimension;
import com.edu.bigdata.transform.dimension.key.base.DateDimension;
import com.edu.bigdata.transform.dimension.key.base.KpiDimension;
import com.edu.bigdata.transform.dimension.key.base.PlatformDimension;
import com.edu.bigdata.transform.dimension.key.stats.StatsCommonDimension;
import com.edu.bigdata.transform.dimension.key.stats.StatsUserDimension;
import com.edu.bigdata.transform.util.TimeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;


/**
 * 思路：思路：HBase 读取数据 --> HBaseInputFormat --> Mapper --> Reducer --> DBOutPutFormat--> 这接写入到 MySql 中
 *
 * @author bruce
 */
public class NewInstallUserMapper extends TableMapper<StatsUserDimension, Text> {
    // Mapper 的 OutPutKey 和 OutPutValue
    // OutPutKey = StatsUserDimension 进行用户分析的组合维度(用户基本分析维度和浏览器分析维度)
    // OutPutValue = Text uuid（字符串）

    private static final Logger logger = Logger.getLogger(NewInstallUserMapper.class);

    // 定义列族
    private byte[] family = EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME;

    // 定义输出 key
    private StatsUserDimension outputKey = new StatsUserDimension();
    // 定义输出 value
    private Text outputValue = new Text();

    // 映射输出 key 中的 StatsCommonDimension(公用维度) 属性，方便后续封装操作
    private StatsCommonDimension statsCommonDimension = this.outputKey.getStatsCommon();

    private long date, endOfDate; // 定义运行天的起始时间戳和结束时间戳
    private long firstThisWeekOfDate, endThisWeekOfDate; // 定义运行天所属周的起始时间戳和结束时间戳
    private long firstThisMonthOfDate, firstDayOfNextMonth; // 定义运行天所属月的起始时间戳和结束时间戳

    // 创建 kpi 维度对象
    private KpiDimension newInstallUsersKpiDimension = new KpiDimension(KpiType.NEW_INSTALL_USER.name);
    private KpiDimension browserNewInstallUsersKpiDimension = new KpiDimension(KpiType.BROWSER_NEW_INSTALL_USER.name);

    // 定义一个特殊占位的浏览器维度对象
    private BrowserDimension defaultBrowserDimension = new BrowserDimension("", "");

    // 初始化操作
    @Override
    protected void setup(Mapper<ImmutableBytesWritable, Result, StatsUserDimension, Text>.Context context)
            throws IOException, InterruptedException {
        // 1、获取参数配置项的上下文
        Configuration conf = context.getConfiguration();
        // 2、获取我们给定的运行时间参数，获取运行的是哪一天的数据
        String date = conf.get(GlobalConstants.RUNNING_DATE_PARAMES);

        // 传入时间所属当前天开始的时间戳，即当前天的0点0分0秒的毫秒值
        this.date = TimeUtil.parseString2Long(date);
        // 传入时间所属当前天结束的时间戳
        this.endOfDate = this.date + GlobalConstants.DAY_OF_MILLISECONDS;
        // 传入时间所属当前周的第一天的时间戳
        this.firstThisWeekOfDate = TimeUtil.getFirstDayOfThisWeek(this.date);
        // 传入时间所属下一周的第一天的时间戳
        this.endThisWeekOfDate = TimeUtil.getFirstDayOfNextWeek(this.date);
        // 传入时间所属当前月的第一天的时间戳
        this.firstThisMonthOfDate = TimeUtil.getFirstDayOfThisMonth(this.date);
        // 传入时间所属下一月的第一天的时间戳
        this.firstDayOfNextMonth = TimeUtil.getFirstDayOfNextMonth(this.date);
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context)
            throws IOException, InterruptedException {
        // 1、获取属性，参数值，即读取 HBase 中的数据：serverTime、platformName、platformVersion、browserName、browserVersion、uuid
        /*String serverTime = Bytes
                .toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)));
        String platformName = Bytes
                .toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM)));
        String platformVersion = Bytes
                .toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_VERSION)));
        String browserName = Bytes
                .toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME)));
        String browserVersion = Bytes
                .toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION)));
        String uuid = Bytes
                .toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_UUID)));*/

        String serverTime = getValueByProperty(value,family,EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME);
        String platformName = getValueByProperty(value,family,EventLogConstants.LOG_COLUMN_NAME_PLATFORM);
        String platformVersion = getValueByProperty(value,family,EventLogConstants.LOG_COLUMN_NAME_VERSION);
        String browserName = getValueByProperty(value,family,EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME);
        String browserVersion = getValueByProperty(value,family,EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION);
        String uuid = getValueByProperty(value,family,EventLogConstants.LOG_COLUMN_NAME_UUID);

        // 2、针对数据进行简单过滤（实际开发中过滤条件更多）
        if (StringUtils.isBlank(platformName) || StringUtils.isBlank(uuid)) {
            logger.debug("数据格式异常，直接过滤掉数据:" + platformName);
            return; // 过滤掉无效数据
        }

        // 属性处理
        long longOfServerTime = -1;
        try {
            longOfServerTime = Long.valueOf(serverTime); // 将字符串转换为long类型
        } catch (Exception e) {
            logger.debug("服务器时间格式异常:" + serverTime);
            return; // 服务器时间异常的数据直接过滤掉
        }

        // 3、构建维度信息
        // 获取当前服务器时间对应的当天维度的对象
        DateDimension dayOfDimension = DateDimension.buildDate(longOfServerTime, DateEnum.DAY);
        // 获取当前服务器时间对应的当周维度的对象
        DateDimension weekOfDimension = DateDimension.buildDate(longOfServerTime, DateEnum.WEEK);
        // 获取当前服务器时间对应的当月维度的对象
        DateDimension monthOfDimension = DateDimension.buildDate(longOfServerTime, DateEnum.MONTH);
        // 还可以获取 当季维度、当年维度......

        // 构建平台维度对象
        List<PlatformDimension> platforms = PlatformDimension.buildList(platformName, platformVersion);
        // 构建浏览器维度对象
        List<BrowserDimension> browsers = BrowserDimension.buildList(browserName, browserVersion);

        // 4、设置 outputValue
        this.outputValue.set(uuid);

        // 5、设置 outputKey
        for (PlatformDimension pf : platforms) {
            // 设置浏览器维度（是个空的）
            this.outputKey.setBrowser(this.defaultBrowserDimension);
            // 设置平台维度
            this.statsCommonDimension.setPlatform(pf);

            // 下面的代码是处理对应于 stats_user 表的统计数据

            // 设置 kpi 维度
            this.statsCommonDimension.setKpi(this.newInstallUsersKpiDimension);

            // 处理不同时间维度的情况
            // 处理天维度数据，要求服务器时间处于指定日期的范围：[today, endOfDate)
            if (longOfServerTime >= date && longOfServerTime < endOfDate) {
                // 设置时间维度为服务器时间当天的维度
                this.statsCommonDimension.setDate(dayOfDimension);
                // 输出数据
                context.write(outputKey, outputValue);
            }

            // 处理周维度数据，范围：[firstThisWeekOfDate, endThisWeekOfDate)
            if (longOfServerTime >= firstThisWeekOfDate && longOfServerTime < endThisWeekOfDate) {
                // 设置时间维度为服务器时间所属周的维度
                this.statsCommonDimension.setDate(weekOfDimension);
                // 输出数据
                context.write(outputKey, outputValue);
            }

            // 处理月维度数据，范围：[firstThisMonthOfDate, firstDayOfNextMonth)
            if (longOfServerTime >= firstThisMonthOfDate && longOfServerTime < firstDayOfNextMonth) {
                // 设置时间维度为服务器时间所属月的维度
                this.statsCommonDimension.setDate(monthOfDimension);
                // 输出数据
                context.write(outputKey, outputValue);
            }

            // 下面的代码是处理对应于 stats_device_browser 表的统计数据

            // 设置 kpi 维度
            this.statsCommonDimension.setKpi(this.browserNewInstallUsersKpiDimension);
            for (BrowserDimension br : browsers) {
                // 设置浏览器维度
                this.outputKey.setBrowser(br);

                // 处理不同时间维度的情况
                // 处理天维度数据，要求当前事件的服务器时间处于指定日期的范围内,[今天0点, 明天0点)
                if (longOfServerTime >= date && longOfServerTime < endOfDate) {
                    // 设置时间维度为服务器时间当天的维度
                    this.statsCommonDimension.setDate(dayOfDimension);
                    // 输出数据
                    context.write(outputKey, outputValue);
                }

                // 处理周维度数据，范围：[firstThisWeekOfDate, endThisWeekOfDate)
                if (longOfServerTime >= firstThisWeekOfDate && longOfServerTime < endThisWeekOfDate) {
                    // 设置时间维度为服务器时间所属周的维度
                    this.statsCommonDimension.setDate(weekOfDimension);
                    // 输出数据
                    context.write(outputKey, outputValue);
                }

                // 处理月维度数据，范围：[firstThisMonthOfDate, firstDayOfNextMonth)
                if (longOfServerTime >= firstThisMonthOfDate && longOfServerTime < firstDayOfNextMonth) {
                    // 设置时间维度为服务器时间所属月的维度
                    this.statsCommonDimension.setDate(monthOfDimension);
                    // 输出数据
                    context.write(outputKey, outputValue);
                }
            }
        }

    }

    private String getValueByProperty(Result value,byte[] family,String property){
        return Bytes.toString(value.getValue(family,Bytes.toBytes(property)));
    }
}