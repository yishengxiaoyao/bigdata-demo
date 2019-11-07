package com.edu.bigdata.transform.mr.etl;

import com.edu.bigdata.transform.common.EventLogConstants;
import com.edu.bigdata.transform.common.EventLogConstants.EventEnum;
import com.edu.bigdata.transform.util.LoggerUtil;
import com.edu.bigdata.transform.util.TimeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.zip.CRC32;


public class AnalysisDataMapper extends Mapper<Object, Text, NullWritable, Put> {
    // Object 是偏移量，Text 表示输入，NullWritable, Put 可以互换

    // 如果无法处理给定的事件类型，则使用 log4j 输出， Logger 可以在运行 jar 包的控制台输出
    private static final Logger logger = Logger.getLogger(AnalysisDataMapper.class);

    private CRC32 crc1 = null;
    private CRC32 crc2 = null;
    private byte[] family = null;
    private long currentDayInMills = -1;

    /**
     * 初始化数据
     */
    @Override
    protected void setup(Mapper<Object, Text, NullWritable, Put>.Context context)
            throws IOException, InterruptedException {
        crc1 = new CRC32();
        crc2 = new CRC32();
        this.family = EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME;
        currentDayInMills = TimeUtil.getTodayInMillis();
    }

    // 1、覆写 map 方法
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // 2、将原始数据通过 LoggerUtil 解析成 Map 键值对
        Map<String, String> clientInfo = LoggerUtil.handleLogText(value.toString());

        // 2.1、如果解析失败，则 Map 集合中无数据，通过日志输出当前数据
        if (clientInfo.isEmpty()) {
            logger.debug("日志解析失败：" + value.toString());
            return;
        }

        // 3、根据解析后的数据，生成对应的 Event 事件类型（通过枚举类型的别名来解析）
        EventEnum event = EventEnum.valueOfAlias(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME));
        if (event == null) {
            // 4、无法处理的事件，直接输出事件类型
            logger.debug("无法匹配对应的事件类型：" + clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME));
        } else {
            // 5、处理具体的事件
            handleEventData(clientInfo, event, context, value);
            // clientInfo 数据集, event 事件类型, context 上下文(通过上下文写入到HBase), value 当前行的数据(可能会有新的过滤操作或者debug时调试方便,可有可无)
        }
    }

    /**
     * 处理具体的事件的方法
     *
     * @param clientInfo
     * @param event
     * @param context
     * @param value
     * @throws InterruptedException
     * @throws IOException
     */
    public void handleEventData(Map<String, String> clientInfo, EventEnum event, Context context, Text value)
            throws IOException, InterruptedException {
        // 6、如果事件成功通过过滤，则准备处理具体事件
        if (filterEventData(clientInfo, event)) {
            outPutData(clientInfo, context);
        } else {
            // 如果事件没有通过过滤，则通过日志输出当前数据
            logger.debug("事件格式不正确：" + value.toString());
        }
    }

    /**
     * 6、如果事件成功通过过滤，则准备处理具体事件（我们的 HBase 只存成功通过过滤的事件）
     *
     * @param clientInfo
     * @param event
     * @return
     */
    public boolean filterEventData(Map<String, String> clientInfo, EventEnum event) {
        // 事件数据全局过滤（具体全局过滤条件视情况而定，这里的 “服务器时间” 和 “平台” 是例子）
        boolean result = StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME))
                && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_PLATFORM));
        // 后面几乎全部是&&操作，只要有一个 false，那么该 Event 事件就无法处理
        if (!result) {
            return result;
        }

        // public static final String PC_WEBSITE_SDK = "website";
        // public static final String JAVA_SERVER_SDK = "java_server";

        // 先确定平台
        switch (clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_PLATFORM)) {
            // Java Server 平台发来的数据
            case EventLogConstants.PlatformNameConstants.JAVA_SERVER_SDK:
                result = result && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_MEMBER_ID)); // 先判断会员 ID 是否存在
                // 再确定事件
                switch (event) {
                    case CHARGEREFUND:
                        // 退款事件
                        // ......
                        break;
                    case CHARGESUCCESS:
                        // 订单支付成功事件
                        result = result && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_ORDER_ID));
                        break;
                    default:
                        logger.debug("无法处理指定事件：" + clientInfo);
                        result = false;
                        break;
                }
                break;

            // WebSite 平台发来的数据
            case EventLogConstants.PlatformNameConstants.PC_WEBSITE_SDK:
                // 再确定事件
                switch (event) {
                    case CHARGEREQUEST:
                        // 下单事件
                        result = result && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_ORDER_ID))
                                && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_ORDER_CURRENCY_TYPE))
                                && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_ORDER_PAYMENT_TYPE))
                                && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_ORDER_CURRENCY_AMOUNT));
                        break;
                    case EVENT:
                        // Event 事件
                        result = result && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_EVENT_CATEGORY))
                                && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_EVENT_ACTION));
                        break;
                    case LAUNCH:
                        // Launch 访问事件
                        // ......
                        break;
                    case PAGEVIEW:
                        // PV 事件
                        result = result && StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_CURRENT_URL));
                        break;
                    default:
                        logger.debug("无法处理指定事件：" + clientInfo);
                        result = false;
                        break;
                }
                break;

            default:
                result = false;
                logger.debug("无法确定的数据来源：" + clientInfo);
                break;
        }

        return result;
    }

    /**
     * 7 和 8、如果事件成功通过过滤，则输出事件到 HBase 的方法
     *
     * @param clientInfo
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void outPutData(Map<String, String> clientInfo, Context context) throws IOException, InterruptedException {
        // 因为浏览器信息已经解析完成，所以此时删除原始的浏览器信息
        clientInfo.remove(EventLogConstants.LOG_COLUMN_NAME_USER_AGENT);

        String uuid = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_UUID);
        long serverTime = Long.valueOf(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME));
        // 创建 rowKey
        byte[] rowkey = generateRowKey(uuid, serverTime, clientInfo);
        Put put = new Put(rowkey);

        for (Map.Entry<String, String> entry : clientInfo.entrySet()) {
            if (StringUtils.isNotBlank(entry.getKey()) && StringUtils.isNotBlank(entry.getValue())) {
                put.addColumn(family, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
            }
        }

        context.write(NullWritable.get(), put);
    }

    /**
     * 9、为向 HBase 中写入数据依赖 Put 对象，Put 对象的创建依赖 RowKey，所以如下方法
     * <p>
     * rowKey=时间+uuid的crc32编码+数据内容的hash码的crc32编码
     *
     * @return
     */
    public byte[] generateRowKey(String uuid, long serverTime, Map<String, String> clientInfo) {
        // 先清空 crc1 和  crc2 集合中的数据内容
        crc1.reset();
        crc2.reset();

        // 时间=当前数据访问服务器的时间-当天00:00点的时间戳 ，得到最大值是8位数字=3600*24*1000=86400000 ，可以用int来存储，大小是 4个字节
        byte[] timeBytes = Bytes.toBytes(serverTime - this.currentDayInMills);

        // uuid 的 crc 编码
        if (StringUtils.isNotBlank(uuid)) {
            this.crc1.update(Bytes.toBytes(uuid));
        }
        byte[] uuidBytes = Bytes.toBytes(this.crc1.getValue());

        // 数据内容的 hash 码的 crc 编码
        this.crc2.update(Bytes.toBytes(clientInfo.hashCode()));
        byte[] clientInfoBytes = Bytes.toBytes(this.crc2.getValue());

        // 综合字节数组
        byte[] buffer = new byte[timeBytes.length + uuidBytes.length + clientInfoBytes.length];
        // 数组合并
        System.arraycopy(timeBytes, 0, buffer, 0, timeBytes.length);
        System.arraycopy(uuidBytes, 0, buffer, timeBytes.length, uuidBytes.length);
        System.arraycopy(clientInfoBytes, 0, buffer, uuidBytes.length, clientInfoBytes.length);

        return buffer;
    }

}
