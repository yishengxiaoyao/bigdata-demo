#电信客服分析平台
## 1.项目背景
&ensp;&ensp;&ensp;&ensp;通信运营商每时每刻会产生大量的通信数据，例如：通话记录，短信记录，彩信记录，第三方服务资费等等繁多信息。数据量如此巨大，除了要满足用户的实时查询和展示之外，还需要定时定期的对已有数据进行离线的分析处理。例如：当日话单，月度话单，季度话单，年度话单，通话详情，通话记录等等。
## 2.项目架构

## 3.项目实现
系统环境:

|系统|版本|
|----|----|
|linux|Centos6.8|
开发工具:

|工具|版本|
|----|----|
|IDEA|2018|
|maven|3.6.0|
|jdk|1.8|
集群环境:

|框架|版本|
|----|----|
|Hadoop|hadoop-2.6.0-CDH5.7.0|
|zookeeper|zookeeper-3.4.5-cdh5.7.0|
|hbase|hbase-1.2.0-CDH5.7.0|
|hive|hive-1.1.0-cdh5.7.0|
|flume|flume-ng-1.6.0-cdh5.7.0|
|kafka|kafka_2.11-0.10.2.2|

### 3.1.产生数据
&ensp;&ensp;&ensp;数据产生是一套完成且严密的体系，这样可以保证数据的鲁棒性。
#### 3.1.1 数据结构
```
call1       call2       date_time       duration
13651311090 18611213803 20171017081520  0360        默认call1是主叫

RowKey:(20到100个字符)

    13651311090_20171017081520_18611213803_0360_1  主叫
    18611213803_20171017081520_13651311090_0360_0  被叫       
```
数据结构如下:

|列名|解释|举例|
|----|----|----|
|call1|第一个手机号码|13651311090|
|call1_name|第一个手机号码人姓名(非必要)|逍遥一生|
|call2|第二个手机号码|18611213803|
|call2_name|第二个手机号码人姓名(非必要)|一生逍遥|
|build_time|建立童话的时间(时间戳:年月日时分秒)|20171017081520|
|duration|通话时间(秒)|0360|
|flag|call1是主叫还是被叫|1主叫,0 被叫|

#### 3.1.2 编写代码
思路:
```
   a)需要存储用户的相关信息:手机号和姓名的关系。
   b)随机选择两个手机号(手机号不能重复)，并将其变成主叫和被叫。
   c)创建随机生成通话建立时间的方法，可指定随机范围，最后生成通话建立时间，产出 date_time 字段数据；
   d) 随机一个通话时长，单位：秒，产出 duration 字段数据；
   e) 将产出的一条数据拼接封装到一个字符串中；
   f) 使用 IO 操作将产出的一条通话数据写入到本地文件中。（一定要手动 flush，这样能确保每条数据写入到文件一次）
```
创建新的Module(telecom-product):
```xml
 <artifactId>telecom-producer</artifactId>
    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
    </dependencies>
```
在编写代码的时候，主要包含一下几个方面:
1.随机生成手机号和联系人，并将其存储(initPhone)。<br>
2.随机生成通话时间(randomBuildTime),时间的格式为"yyyy-MM-dd HH:mm:ss",以及开始和结束时间。<br>
3.生成一条日志(productLog),将电话号码、建立时间、通话时长等拼成一个字符串,call1_buildTime_call2_duration_flag。<br>
4.将日志写入到文件中。
```java
package com.edu.bigdata.producer;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 产生数据
 */

public class ProductLog {

    private String startTime = "2017-01-01";
    private String endTime = "2017-12-31";

    // 用于存放待随机的联系人电话
    private List<String> phoneList = new ArrayList<>();

    // 用于存放联系人电话与姓名的映射
    private Map<String, String> phoneNameMap = new HashMap<>();

    public static void main(String[] args) throws InterruptedException {

        if (args == null || args.length <= 0) {
            System.out.println("No arguments");
            return;
        }

        ProductLog productLog = new ProductLog();
        productLog.initPhone();
        productLog.writeLog(args[0]);
    }

    /**
     * 初始化随机的电话号码和姓名
     */
    public void initPhone() {
        phoneList.add("13242820024");
        phoneList.add("14036178412");
        phoneList.add("16386074226");
        phoneList.add("13943139492");
        phoneList.add("18714767399");
        phoneList.add("14733819877");
        phoneList.add("13351126401");
        phoneList.add("13017498589");
        phoneList.add("16058589347");
        phoneList.add("18949811796");
        phoneList.add("13558773808");
        phoneList.add("14343683320");
        phoneList.add("13870632301");
        phoneList.add("13465110157");
        phoneList.add("15382018060");
        phoneList.add("13231085347");
        phoneList.add("13938679959");
        phoneList.add("13779982232");
        phoneList.add("18144784030");
        phoneList.add("18637946280");

        phoneNameMap.put("13242820024", "李雁");
        phoneNameMap.put("14036178412", "卫艺");
        phoneNameMap.put("16386074226", "仰莉");
        phoneNameMap.put("13943139492", "陶欣悦");
        phoneNameMap.put("18714767399", "施梅梅");
        phoneNameMap.put("14733819877", "金虹霖");
        phoneNameMap.put("13351126401", "魏明艳");
        phoneNameMap.put("13017498589", "华贞");
        phoneNameMap.put("16058589347", "华啟倩");
        phoneNameMap.put("18949811796", "仲采绿");
        phoneNameMap.put("13558773808", "卫丹");
        phoneNameMap.put("14343683320", "戚丽红");
        phoneNameMap.put("13870632301", "何翠柔");
        phoneNameMap.put("13465110157", "钱溶艳");
        phoneNameMap.put("15382018060", "钱琳");
        phoneNameMap.put("13231085347", "缪静欣");
        phoneNameMap.put("13938679959", "焦秋菊");
        phoneNameMap.put("13779982232", "吕访琴");
        phoneNameMap.put("18144784030", "沈丹");
        phoneNameMap.put("18637946280", "褚美丽");
    }

    /**
     * 根据传入的时间区间，在此范围内随机产生通话建立的时间
     * 公式：startDate.getTime() + (endDate.getTime() - startDate.getTime()) * Math.random()
     *
     * @param startTime
     * @param endTime
     * @return
     */
    public String randomBuildTime(String startTime, String endTime) {
        try {
            SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
            Date startDate = sdf1.parse(startTime);
            Date endDate = sdf1.parse(endTime);

            if (endDate.getTime() <= startDate.getTime()) {
                return null;
            }

            long randomTS = startDate.getTime() + (long) ((endDate.getTime() - startDate.getTime()) * Math.random());

            Date resultDate = new Date(randomTS);
            SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String resultTimeString = sdf2.format(resultDate);

            return resultTimeString;

        } catch (ParseException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 生产数据的形式：13651311090,18611213803,2017-10-17 08:15:20,0360
     */
    public String productLog() {

        String caller = null;
        String callee = null;

        String callerName = null;
        String calleeName = null;

        // 随机获取主叫手机号
        int callerIndex = (int) (Math.random() * phoneList.size()); // [0, 20)
        caller = phoneList.get(callerIndex);
        callerName = phoneNameMap.get(caller);

        // 随机获取被叫手机号
        while (true) {
            int calleeIndex = (int) (Math.random() * phoneList.size()); // [0, 20)
            callee = phoneList.get(calleeIndex);
            calleeName = phoneNameMap.get(callee);

            if (!caller.equals(callee)) {
                break;
            }
        }

        // 随机获取通话建立的时间
        String buildTime = randomBuildTime(startTime, endTime);

        // 随机获取通话的时长
        DecimalFormat df = new DecimalFormat("0000");
        String duration = df.format((int) (30 * 60 * Math.random()));

        StringBuilder sb = new StringBuilder();
        sb.append(caller + ",").append(callee + ",").append(buildTime + ",").append(duration);
        return sb.toString();
    }

    /**
     * 将数据写入到文件中
     */
    public void writeLog(String filePath) {
        try {
            OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(filePath), StandardCharsets.UTF_8);

            while (true) {
                Thread.sleep(200);

                String log = productLog();

                osw.write(log + "\n");
                osw.flush(); // 一定要手动flush，这样能确保每条数据写入到文件一次
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
```
#### 3.1.3打包测试
1.打包方式:

使用maven命令打包，如果设置了不同的环境参数的话，就可以根据环境不同来打不同的包。
```
maven -U clean package -Dmaven.test.skip=true
```

2.定时执行

将生成日志的任务写成bash脚本:
```
#!/bin/bash
java -cp /opt/module/flume/job/ct/ct_producer-1.0-SNAPSHOT.jar com.edu.bigdata.producer.ProductLog /opt/module/flume/job/ct/calllog.csv
```

###3.2 数据采集/消费(存储)

