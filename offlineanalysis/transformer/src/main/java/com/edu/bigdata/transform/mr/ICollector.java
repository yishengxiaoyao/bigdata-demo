package com.edu.bigdata.transform.mr;

import com.edu.bigdata.transform.converter.IDimensionConverter;
import com.edu.bigdata.transform.dimension.base.BaseDimension;
import com.edu.bigdata.transform.dimension.value.BaseStatsValue;
import org.apache.hadoop.conf.Configuration;

import java.sql.PreparedStatement;

public interface ICollector {
    void collect(Configuration conf, BaseDimension key, BaseStatsValue value, PreparedStatement ps, IDimensionConverter converter);
}
