package com.edu.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class PsuedoRank extends GenericUDF {

    private long rank;

    private String[] groupKey;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] currentKey) throws HiveException {
        if (!sameAsPreviousKey(currentKey)){
            rank = 1;
        }
        return new Long(rank++);
    }

    private boolean sameAsPreviousKey(DeferredObject[] currentKey)throws HiveException{
        if(null == currentKey && null == groupKey){
            return true;
        }
        String[] previousKey = groupKey;

        copy(currentKey);

        if (null == groupKey&& null!=previousKey){
            return false;
        }

        if (null!=groupKey&&null==previousKey){
            return false;
        }

        if(groupKey.length!=previousKey.length){
            return false;
        }

        for (int index = 0; index < previousKey.length;index++){
            if(!groupKey[index].equalsIgnoreCase(previousKey[index])){
                return false;
            }
        }
        return true;


    }

    private void copy(DeferredObject[] currentKey)throws HiveException{
        if (null == currentKey){
            groupKey = null;
        }else {
            groupKey = new String[currentKey.length];
            for (int index = 0;index < currentKey.length; index++){
                groupKey[index] = String.valueOf(currentKey[index].get());
            }
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        StringBuilder sb = new StringBuilder();
        sb.append(" PsuedoRank (");
        for (int i=0;i< children.length;i++){
            if(i>0){
                sb.append(",");
            }
            sb.append(children[i]);
        }
        sb.append(")");
        return sb.toString();

    }
}
