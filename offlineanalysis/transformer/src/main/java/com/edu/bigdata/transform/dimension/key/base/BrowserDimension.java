package com.edu.bigdata.transform.dimension.key.base;

import com.edu.bigdata.transform.dimension.base.BaseDimension;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BrowserDimension extends BaseDimension {

    private int id;
    private String browerName;
    private String browerVersion;

    public BrowserDimension() {
    }

    public BrowserDimension(String browerName, String browerVersion) {
        super();
        this.browerName = browerName;
        this.browerVersion = browerVersion;
    }

    public static List<BrowserDimension> buildList(String browerName, String browerVersion) {
        List<BrowserDimension> browserDimensions = new ArrayList<>();
        BrowserDimension bd = new BrowserDimension(browerName,browerVersion);
        browserDimensions.add(bd);
        return browserDimensions;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getBrowerVersion() {
        return browerVersion;
    }

    public void setBrowerVersion(String browerVersion) {
        this.browerVersion = browerVersion;
    }

    public String getBrowerName() {
        return browerName;
    }

    public void setBrowerName(String browerName) {
        this.browerName = browerName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o){
            return true;
        }
        if (o == null || getClass() != o.getClass()){
            return false;
        }

        BrowserDimension that = (BrowserDimension) o;

        if (this.browerName != null ? !this.browerName.equals(that.getBrowerName()) : that.getBrowerName() != null) return false;
        return this.browerVersion != null ? this.browerVersion.equals(that.getBrowerVersion()) : that.getBrowerVersion() == null;
    }

    @Override
    public int hashCode() {
        int result = this.browerName != null ? this.browerName.hashCode():0;
        result = 31 * result + (this.browerVersion != null ? this.browerVersion.hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (o == this){
            return 0;
        }
        BrowserDimension bd = (BrowserDimension) o;
        int result = Integer.compare(this.id,bd.getId());
        if (result != 0){
            return result;
        }
        result = this.browerName.compareTo(bd.getBrowerName());
        if (result != 0){
            return result;
        }
        result = this.browerVersion.compareTo(bd.getBrowerVersion());
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (out != null) {
            out.writeInt(this.id);
            Text.writeString(out, this.browerName);
            Text.writeString(out, this.browerVersion);
        }
        //out.writeUTF(this.browerName);
        //out.writeUTF(this.browerVersion);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        if (in != null) {
           // this.browerName = in.readUTF();
           // this.browerVersion = in.readUTF();
            this.id = in.readInt();
            this.browerName = Text.readString(in);
            this.browerVersion = Text.readString(in);
        }
    }
}
