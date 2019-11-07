package com.edu.bigdata.transform.udf;

import com.edu.bigdata.transform.converter.IDimensionConverter;
import com.edu.bigdata.transform.converter.impl.DimensionConverterImpl;
import com.edu.bigdata.transform.dimension.key.base.KpiDimension;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.IOException;

/**
 * 用于根据 kpi 名称获取 kpi 维度 id
 */
public class KpiDimensionConverterUDF extends UDF {
    // 用于根据维度值获取维度id的对象
    private IDimensionConverter converter = null;

    /**
     * 默认无参构造方法，必须给定的
     */
    public KpiDimensionConverterUDF() {
        // 初始化操作
        this.converter = new DimensionConverterImpl();
    }

    /**
     * @param kpiName
     * @return
     * @throws IOException
     */
    public int evaluate(String kpiName) throws IOException {
        // 1、判断参数是否为空
        if (StringUtils.isBlank(kpiName)) {
            throw new IllegalArgumentException("参数异常，kpiName不能为空!!!");
        }
        // 2、构建 kpi 对象
        KpiDimension kpi = new KpiDimension(kpiName);
        // 3、获取 id 的值
        return this.converter.getDimensionIdByValue(kpi);
    }
}