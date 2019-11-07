package com.edu.bigdata.transform.dimension.key.base;

import com.edu.bigdata.transform.dimension.base.BaseDimension;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KpiDimension extends BaseDimension {

    private int id;
    private String kpiName;

    public KpiDimension(String kpiName) {
        this.kpiName = kpiName;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getKpiName() {
        return kpiName;
    }

    public void setKpiName(String kpiName) {
        this.kpiName = kpiName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o){
            return true;
        }

        if (o == null || getClass() != o.getClass()){
            return false;
        }

        KpiDimension kd = (KpiDimension) o;
        return this.kpiName != null ? this.kpiName.equals(kd.getKpiName()) : kd.getKpiName() != null;
    }

    @Override
    public int hashCode() {
        return this.kpiName != null ? this.kpiName.hashCode() : 0;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (o == this){
            return  0;
        }
        KpiDimension kd = (KpiDimension) o;
        int result = Integer.compare(this.id,kd.getId());
        if (result != 0){
            return result;
        }
        result = this.kpiName.compareTo(kd.getKpiName());
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (out != null){
            //out.writeUTF(this.kpiName);
            out.writeInt(this.id);
            Text.writeString(out,this.kpiName);
        }

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        //this.kpiName = in.readUTF();
        if (in != null){
            this.id = in.readInt();
            this.kpiName = Text.readString(in);
        }
    }
}
