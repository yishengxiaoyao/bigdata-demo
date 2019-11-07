package com.edu.bigdata.analysis.convert;

import com.edu.bigdata.analysis.kv.base.BaseDimension;

public interface DimensionConverter {

    /**
     * 根据传入的 baseDimension 对象，获取数据库中对应该对象数据的id，如果不存在，则插入该数据再返回
     */
    int getDimensionId(BaseDimension baseDimension);
}