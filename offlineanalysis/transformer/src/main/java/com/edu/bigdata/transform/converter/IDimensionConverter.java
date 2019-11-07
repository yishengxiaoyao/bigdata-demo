package com.edu.bigdata.transform.converter;

import com.edu.bigdata.transform.dimension.base.BaseDimension;

public interface IDimensionConverter {
    int getDimensionIdByValue(BaseDimension baseDimension);
}
