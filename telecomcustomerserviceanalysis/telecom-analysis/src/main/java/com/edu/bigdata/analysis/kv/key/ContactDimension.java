package com.edu.bigdata.analysis.kv.key;

import com.edu.bigdata.analysis.kv.base.BaseDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 联系人维度类
 */
public class ContactDimension extends BaseDimension {

    // 联系人维度主键
    private int id;

    // 联系人维度：手机号码
    private String telephone;

    // 联系人维度：姓名
    private String name;

    public ContactDimension() {
        super();
    }

    public ContactDimension(String telephone, String name) {
        super();
        this.telephone = telephone;
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getTelephone() {
        return telephone;
    }

    public void setTelephone(String telephone) {
        this.telephone = telephone;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ContactDimension that = (ContactDimension) o;

        if (telephone != null ? !telephone.equals(that.telephone) : that.telephone != null) return false;
        return name != null ? name.equals(that.name) : that.name == null;
    }

    @Override
    public int hashCode() {
        int result = telephone != null ? telephone.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (o == this) return 0;
        ContactDimension anotherContactDimension = (ContactDimension) o;

        int result = Integer.compare(this.id, anotherContactDimension.getId());
        if (result != 0) return result;

        result= this.telephone.compareTo(anotherContactDimension.getTelephone());
        if (result != 0) return result;

        result = this.name.compareTo(anotherContactDimension.getName());
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        dataOutput.writeUTF(this.telephone);
        dataOutput.writeUTF(this.name);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.telephone = dataInput.readUTF();
        this.name = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return "ContactDimension{" +
                "id=" + id +
                ", telephone='" + telephone + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}