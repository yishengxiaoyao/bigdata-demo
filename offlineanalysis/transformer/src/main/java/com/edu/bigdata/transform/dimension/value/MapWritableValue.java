package com.edu.bigdata.transform.dimension.value;

import com.edu.bigdata.transform.common.KpiType;
import com.edu.bigdata.transform.dimension.base.BaseValue;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MapWritableValue extends BaseValue {

    private MapWritable value;
    private KpiType kpi;

    public MapWritable getValue() {
        return value;
    }

    public void setValue(MapWritable value) {
        this.value = value;
    }

    public KpiType getKpi() {
        return kpi;
    }

    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        this.value.write(out);
        WritableUtils.writeEnum(out,this.kpi);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.value.readFields(in);
        WritableUtils.readEnum(in,KpiType.class);
    }
}
