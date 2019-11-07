package com.edu.bigdata.transform.mr.collector;

import com.edu.bigdata.transform.converter.IDimensionConverter;
import com.edu.bigdata.transform.dimension.base.BaseDimension;
import com.edu.bigdata.transform.dimension.value.BaseStatsValue;
import com.edu.bigdata.transform.mr.ICollector;
import org.apache.hadoop.conf.Configuration;

import java.sql.PreparedStatement;

public class NewInstallUserCollector implements ICollector {

    public NewInstallUserCollector() {
    }

    @Override
    public void collect(Configuration conf, BaseDimension key, BaseStatsValue value, PreparedStatement ps, IDimensionConverter converter) {

    }
}
