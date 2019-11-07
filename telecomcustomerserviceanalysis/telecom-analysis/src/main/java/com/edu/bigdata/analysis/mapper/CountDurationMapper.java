package com.edu.bigdata.analysis.mapper;


import com.edu.bigdata.analysis.kv.key.ComDimension;
import com.edu.bigdata.analysis.kv.key.ContactDimension;
import com.edu.bigdata.analysis.kv.key.DateDimension;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CountDurationMapper extends TableMapper<ComDimension, Text> {

    private ComDimension comDimension = new ComDimension();

    private Text durationText = new Text();

    // 用于存放联系人电话与姓名的映射
    private Map<String, String> phoneNameMap = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        phoneNameMap = new HashMap<>();

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

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        // 01_15837312345_20170810141024_13738909097_1_0180

        // 获取数据
        String roeKey = Bytes.toString(value.getRow());

        // 切割
        String[] splits = roeKey.split("_");

        // 只拿到主叫数据即可
        String flag = splits[4];
        if (flag.equals("0")) return;

        String call1 = splits[1];
        String call2 = splits[3];
        String bulidTime = splits[2];
        String duration = splits[5];

        durationText.set(duration);

        int year = Integer.valueOf(bulidTime.substring(0, 4));
        int month = Integer.valueOf(bulidTime.substring(4, 6));
        int day = Integer.valueOf(bulidTime.substring(6, 8));

        // 组装-时间维度类DateDimension
        DateDimension yearDimension = new DateDimension(year, -1, -1);
        DateDimension monthDimension = new DateDimension(year, month, -1);
        DateDimension dayDimension = new DateDimension(year, month, day);

        // 组装-联系人维度类ContactDimension
        ContactDimension call1ContactDimension = new ContactDimension(call1, phoneNameMap.get(call1)); // 实际业务做法：1、不写name。2、在Mapper这里调用HBase的API去HBase中将名字和手机号的映射读出来。
        ContactDimension call2ContactDimension = new ContactDimension(call2, phoneNameMap.get(call2)); // 学习阶段，为了数据好看和省事，我们简单做一下

        // 组装-组合维度类ComDimension
        // 聚合主叫数据
        comDimension.setContactDimension(call1ContactDimension);
        // 年
        comDimension.setDateDimension(yearDimension);
        context.write(comDimension, durationText);
        // 月
        comDimension.setDateDimension(monthDimension);
        context.write(comDimension, durationText);
        // 日
        comDimension.setDateDimension(dayDimension);
        context.write(comDimension, durationText);

        // 聚合被叫数据
        comDimension.setContactDimension(call2ContactDimension);
        // 年
        comDimension.setDateDimension(yearDimension);
        context.write(comDimension, durationText);
        // 月
        comDimension.setDateDimension(monthDimension);
        context.write(comDimension, durationText);
        // 日
        comDimension.setDateDimension(dayDimension);
        context.write(comDimension, durationText);
    }
}