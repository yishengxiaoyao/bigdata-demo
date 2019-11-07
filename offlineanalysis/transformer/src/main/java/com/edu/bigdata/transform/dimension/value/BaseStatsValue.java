package com.edu.bigdata.transform.dimension.value;

import com.edu.bigdata.transform.common.KpiType;
import com.edu.bigdata.transform.dimension.base.BaseValue;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BaseStatsValue extends BaseValue {

    private KpiType kpi;

    public KpiType getKpi() {
        return kpi;
    }

    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (out != null) {
            WritableUtils.writeEnum(out, this.kpi);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        if (in != null) {
            this.kpi = WritableUtils.readEnum(in, KpiType.class);
        }
    }
}
