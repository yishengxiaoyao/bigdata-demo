package com.edu.bigdata.transform.dimension.key.stats;

import com.edu.bigdata.transform.dimension.base.BaseDimension;
import com.edu.bigdata.transform.dimension.key.base.DateDimension;
import com.edu.bigdata.transform.dimension.key.base.KpiDimension;
import com.edu.bigdata.transform.dimension.key.base.PlatformDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StatsCommonDimension extends BaseDimension {

    private PlatformDimension platform;
    private KpiDimension kpi;
    private DateDimension date;

    public DateDimension getDate() {
        return date;
    }

    public void setDate(DateDimension date) {
        this.date = date;
    }

    public KpiDimension getKpi() {
        return kpi;
    }

    public void setKpi(KpiDimension kpi) {
        this.kpi = kpi;
    }


    public PlatformDimension getPlatform() {
        return platform;
    }

    public void setPlatform(PlatformDimension platform) {
        this.platform = platform;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o){
            return 0;
        }
        StatsCommonDimension scd = (StatsCommonDimension) o;
        int result = this.platform.compareTo(scd.getPlatform());
        if (result != 0){
            return result;
        }
        result = this.kpi.compareTo(scd.getKpi());
        if (result != 0){
            return result;
        }
        result = this.date.compareTo(scd.getDate());
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (out != null){
            this.platform.write(out);
            this.kpi.write(out);
            this.date.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        if (in != null){
            this.platform.readFields(in);
            this.kpi.readFields(in);
            this.date.readFields(in);
        }
    }
}
