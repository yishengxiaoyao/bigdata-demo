package com.edu.bigdata.transform.mr.statistics;

import com.edu.bigdata.transform.common.KpiType;
import com.edu.bigdata.transform.dimension.key.stats.StatsUserDimension;
import com.edu.bigdata.transform.dimension.value.MapWritableValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class NewInstallUserReducer extends Reducer<StatsUserDimension, Text, StatsUserDimension, MapWritableValue> {

    // 保存唯一 id 的集合 Set，用于计算新增的访客数量
    private Set<String> uniqueSets = new HashSet<String>();

    // 定义输出 value
    private MapWritableValue outputValue = new MapWritableValue();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // 1、统计 uuid 出现的次数，去重
        for (Text uuid : values) { // 增强 for 循环，遍历 values
            this.uniqueSets.add(uuid.toString());
        }

        // 2、输出数据拼装
        MapWritable map = new MapWritable();
        map.put(new IntWritable(-1), new IntWritable(this.uniqueSets.size()));
        this.outputValue.setValue(map);

        // 3、设置 outputValue 数据对应描述的业务指标(kpi)
        if (KpiType.BROWSER_NEW_INSTALL_USER.name.equals(key.getStatsCommon().getKpi().getKpiName())) {
            // 表示处理的是 browser new install user kpi 的计算
            this.outputValue.setKpi(KpiType.BROWSER_NEW_INSTALL_USER);
        } else if (KpiType.NEW_INSTALL_USER.name.equals(key.getStatsCommon().getKpi().getKpiName())) {
            // 表示处理的是 new install user kpi 的计算
            this.outputValue.setKpi(KpiType.NEW_INSTALL_USER);
        }

        // 4、输出数据
        context.write(key, outputValue);
    }
}