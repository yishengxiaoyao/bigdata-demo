package com.edu.bigdata.transform.dimension.key.base;

import com.edu.bigdata.transform.dimension.base.BaseDimension;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PlatformDimension extends BaseDimension {

    private int id;
    private String platformName;
    private String platformVersion;

    public PlatformDimension(String platformName, String platformVersion) {
        this.platformName = platformName;
        this.platformVersion = platformVersion;
    }

    public static List<PlatformDimension> buildList(String platformName, String platformVersion) {
        List<PlatformDimension> platformDimensions = new ArrayList<>();
        PlatformDimension pd = new PlatformDimension(platformName,platformVersion);
        platformDimensions.add(pd);
        return platformDimensions;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPlatformName() {
        return platformName;
    }

    public void setPlatformName(String platformName) {
        this.platformName = platformName;
    }

    public String getPlatformVersion() {
        return platformVersion;
    }

    public void setPlatformVersion(String platformVersion) {
        this.platformVersion = platformVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o){
            return true;
        }
        if (o == null || getClass() != o.getClass()){
            return false;
        }

        PlatformDimension that = (PlatformDimension) o;

        if (this.platformName != null ? !this.platformName.equals(that.getPlatformName()) : that.getPlatformName() != null) return false;
        return this.platformVersion != null ? this.platformVersion.equals(that.getPlatformVersion()) : that.getPlatformVersion() == null;
    }

    @Override
    public int hashCode() {
        int result = this.platformName != null ? this.platformName.hashCode() : 0;
        result = 31 * result + (this.platformVersion != null ? this.platformVersion.hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (o == this) return 0;
        PlatformDimension pd = (PlatformDimension) o;

        int result = Integer.compare(this.id, pd.getId());
        if (result != 0) return result;

        result= this.platformName.compareTo(pd.getPlatformName());
        if (result != 0) return result;

        result = this.platformVersion.compareTo(pd.getPlatformVersion());
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        //out.writeUTF(this.platformName);
        //out.writeUTF(this.platformVersion);
        if (out != null){
            out.writeInt(this.id);
            Text.writeString(out,this.platformName);
            Text.writeString(out,this.platformVersion);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        //this.platformName = in.readUTF();
        //this.platformVersion = in.readUTF();
        if (in != null){
            this.id = in.readInt();
            this.platformName = Text.readString(in);
            this.platformVersion = Text.readString(in);
        }
    }
}
