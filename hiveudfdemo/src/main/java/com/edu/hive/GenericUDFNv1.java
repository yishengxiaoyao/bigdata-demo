package com.edu.hive;

import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

@Description(name = "nv1")
public class GenericUDFNv1 extends UDF {
    private GenericUDFUtils.ReturnObjectInspectorResolver returnObjectInspectorResolver;
    private ObjectInspector[] argumentOIs;

    public ObjectInspector initialize(ObjectInspector[] arguments)throws UDFArgumentException {
        argumentOIs=arguments;
        if(arguments.length!=2){
            throw new UDFArgumentLengthException("1233");
        }
        returnObjectInspectorResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
        if(!(returnObjectInspectorResolver.update(arguments[0])&&returnObjectInspectorResolver.update(arguments[1]))){
            throw new UDFArgumentTypeException(2,"123");
        }
        return returnObjectInspectorResolver.get();
    }

    public Object evaluate(GenericUDF.DeferredObject[] arguments)throws HiveException {
        Object retVal = returnObjectInspectorResolver.convertIfNecessary(arguments[0].get(),argumentOIs[0]);
        if(retVal==null){
            retVal=returnObjectInspectorResolver.convertIfNecessary(arguments[1].get(),argumentOIs[1]);
        }
        return retVal;
    }

    public String getDisplayString(String[] children){
        StringBuffer sb=new StringBuffer();
        sb.append("if ");
        sb.append(children[0]);
        sb.append(" is null ");
        sb.append("returns");
        sb.append(children[1]);
        return sb.toString();
    }
}
