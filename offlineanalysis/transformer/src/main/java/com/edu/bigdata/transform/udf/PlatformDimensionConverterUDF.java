package com.edu.bigdata.transform.udf;

import com.edu.bigdata.transform.common.GlobalConstants;
import com.edu.bigdata.transform.converter.IDimensionConverter;
import com.edu.bigdata.transform.converter.impl.DimensionConverterImpl;
import com.edu.bigdata.transform.dimension.key.base.PlatformDimension;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.IOException;

/**
 * 自定义根据平台维度信息获取维度 id 自定义 udf 的时候，如果使用到 FileSystem（HDFS 的 api），记住一定不要调用 close 方法
 */
public class PlatformDimensionConverterUDF extends UDF {

    // 用于根据维度值获取维度 id 的对象
    private IDimensionConverter converter = null;

    /**
     * 默认无参构造方法，必须给定的
     */
    public PlatformDimensionConverterUDF() {
        // 初始化操作
        this.converter = new DimensionConverterImpl();
    }

    /**
     * 根据给定的平台维度名称和平台维度版本获取对应的维度 id 值
     *
     * @param platformName    维度名称
     * @param platformVersion 维度版本
     * @return
     * @throws IOException 获取 id 的时候产生的异常
     */
    public int evaluate(String platformName, String platformVersion) throws IOException {
        // 1、要求参数不能为空，当为空的时候，设置为 unknown 默认值
        if (StringUtils.isBlank(platformName) || StringUtils.isBlank(platformVersion)) {
            platformName = GlobalConstants.PLATFORM_NAME_DEFAULT_VALUE;
            platformVersion = GlobalConstants.PLATFORM_VERSION_DEFAULT_VALUE;
        }
        // 2、构建一个对象
        PlatformDimension pf = new PlatformDimension(platformName, platformVersion);
        // 3、获取维度 id 值，使用写好的 DimensionConverterImpl 类解析
        return this.converter.getDimensionIdByValue(pf);
    }
}