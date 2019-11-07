package com.edu.bigdata.transform.dimension.key.stats;

import com.edu.bigdata.transform.dimension.base.BaseDimension;
import com.edu.bigdata.transform.dimension.key.base.BrowserDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StatsUserDimension extends BaseDimension {

    private StatsCommonDimension statsCommon;

    private BrowserDimension browser;

    public BrowserDimension getBrowser() {
        return browser;
    }

    public void setBrowser(BrowserDimension browser) {
        this.browser = browser;
    }

    public StatsCommonDimension getStatsCommon() {
        return statsCommon;
    }

    public void setStatsCommon(StatsCommonDimension statsCommon) {
        this.statsCommon = statsCommon;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o){
            return 0;
        }
        StatsUserDimension sud = (StatsUserDimension) o;
        int result = this.statsCommon.compareTo(sud.getStatsCommon());
        if (result != 0){
            return result;
        }
        result = this.browser.compareTo(sud.getBrowser());
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.statsCommon.write(out);
        this.browser.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.statsCommon.readFields(in);
        this.browser.readFields(in);
    }
}
