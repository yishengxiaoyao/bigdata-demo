package com.edu.bigdata.transform.util;

import com.edu.bigdata.transform.common.EventLogConstants;
import com.edu.bigdata.transform.util.IPSeekerExt.RegionInfo;
import cz.mallat.uasparser.UserAgentInfo;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

public class LoggerUtil {
    // 日志输出提示
    private static final Logger logger = LoggerFactory.getLogger(LoggerUtil.class);

    /**
     * 解析给定的日志行，如果解析成功返回一个有值的 map 集合，如果解析失败，返回一个 empty 集合
     *
     * @param logText
     * @return
     */
    public static Map<String, String> handleLogText(String logText) {
        Map<String, String> result = new HashMap<String, String>();
        // 1、开始解析
        // hadoop 集群中默认只有 org.apache.commons.lang.StringUtils 所在的 jar 包，如果使用其他
        // StringUtils，hadoop 集群中需要导入该 StringUtils 依赖的 jar 包方可使用
        if (StringUtils.isNotBlank(logText)) {
            // 日志行非空，可以进行解析
            String[] splits = logText.trim().split(EventLogConstants.LOG_SEPARTIOR); // 日志分隔符
            // ^A
            // 192.168.25.102^A1555318954.798^A/what.png?u_nu=1&u_sd=6D4F89C0-E17B-45D0-BFE0-059644C1878D&c_time=......
            if (splits.length == 3) {
                // 日志格式是正确的，进行解析
                String ip = splits[0].trim();
                // 将 ip 地址封装进 Map 集合中
                result.put(EventLogConstants.LOG_COLUMN_NAME_IP, ip);
                long serverTime = TimeUtil.parseNginxServerTime2Long(splits[1].trim());
                if (serverTime != -1L) {
                    // 表示服务器时间解析正确，而且 serverTime 就是对于的毫秒级的时间戳
                    // 将 serverTime 封装进 Map 集合中
                    result.put(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME, String.valueOf(serverTime));
                }

                // 获取请求体
                String requestBody = splits[2].trim();
                int index = requestBody.indexOf("?"); // ? 符号所在的索引位置
                if (index >= 0 && index != requestBody.length() - 1) {
                    // 在请求参数中存在 ?，而且 ? 不是最后一个字符的情况，则截取?后面的内容
                    requestBody = requestBody.substring(index + 1);
                } else {
                    requestBody = null;
                }

                if (StringUtils.isNotBlank(requestBody)) {
                    // 非空，开始处理请求参数
                    handleRequestBody(result, requestBody);

                    // 开始补全 ip 地址
                    RegionInfo info = IPSeekerExt.getInstance().analysisIp(result.get(EventLogConstants.LOG_COLUMN_NAME_IP)); // 用户ip地址
                    if (info != null) {
                        result.put(EventLogConstants.LOG_COLUMN_NAME_COUNTRY, info.getCountry()); // 国家
                        result.put(EventLogConstants.LOG_COLUMN_NAME_PROVINCE, info.getProvince()); // 省份
                        result.put(EventLogConstants.LOG_COLUMN_NAME_CITY, info.getCity()); // 城市
                    }

                    // 开始补全浏览器信息
                    UserAgentInfo uaInfo = UserAgentUtil.analyticUserAgent(result.get(EventLogConstants.LOG_COLUMN_NAME_USER_AGENT)); // 浏览器user agent参数
                    if (uaInfo != null) {
                        // 浏览器名称
                        result.put(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME, uaInfo.getUaFamily()); // 浏览器名称
                        // 浏览器版本号
                        result.put(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION, uaInfo.getBrowserVersionInfo()); // 浏览器版本
                        // 浏览器所在操作系统
                        result.put(EventLogConstants.LOG_COLUMN_NAME_OS_NAME, uaInfo.getOsFamily()); // 操作系统名称
                        // 浏览器所在操作系统的版本
                        result.put(EventLogConstants.LOG_COLUMN_NAME_OS_VERSION, uaInfo.getOsName()); // 操作系统版本
                    }

                } else {
                    // logger
                    logger.debug("请求参数为空:" + logText);
                    result.clear(); // 清空
                }
            } else {
                // log记录一下
                logger.debug("日志行内容格式不正确:" + logText);
            }
        } else {
            logger.debug("日志行内容为空，无法进行解析:" + logText);
        }
        return result;
    }

    /**
     * 处理请求参数<br/>
     * 处理结果保存到参数 result 集合（Map 集合）
     *
     * @param clientInfo  保存最终用户行为数据的 map 集合
     * @param requestBody 请求参数中，用户行为数据，格式为:
     *                    u_nu=1&u_sd=6D4F89C0-E17B-45D0-BFE0-059644C1878D&c_time=
     *                    1450569596991&ver=1&en=e_l&pl=website&sdk=js&b_rst=1440*900&
     *                    u_ud=4B16B8BB-D6AA-4118-87F8-C58680D22657&b_iev=Mozilla%2F5.0%
     *                    20(Windows%20NT%205.1)%20AppleWebKit%2F537.36%20(KHTML%2C%
     *                    20like%20Gecko)%20Chrome%2F45.0.2454.101%20Safari%2F537.36&l=
     *                    zh-CN&bf_sid=33cbf257-3b11-4abd-ac70-c5fc47afb797_11177014
     */
    private static void handleRequestBody(Map<String, String> clientInfo, String requestBody) {
        // 将请求参数体按照 & 切割
        String[] parameters = requestBody.split("&");
        for (String parameter : parameters) {
            // 循环处理参数，parameter 格式为: c_time=1450569596991  = 只会出现一次
            String[] params = parameter.split("=");
            String key, value = null;
            try {
                // 使用 utf-8 解码
                key = URLDecoder.decode(params[0].trim().replaceAll("%(?![0-9a-fA-F]{2})", "%25"), "utf-8");
                value = URLDecoder.decode(params[1].trim().replaceAll("%(?![0-9a-fA-F]{2})", "%25"), "utf-8");
                // 添加到结果集合  Map 中
                clientInfo.put(key, value);
            } catch (Exception e) {
                logger.warn("解码失败:" + parameter, e);
            }
        }
    }
}
