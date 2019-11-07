package com.edu.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.SimpleDateFormat;
import java.util.Date;
@Description(name = "zodiac",
    value = "_FUNC_(date) - from the input date string or separate month and day argument,returns the sign of the Zodiac.",
    extended = "Example: select _FUNC_(date_string) from src;")
public class UDFZodiacSign extends UDF {

    private SimpleDateFormat df;

    public UDFZodiacSign() {
        this.df=new SimpleDateFormat("yyyy-MM-dd");
    }

    public String evaluate(Date bday){
        return this.evaluate(bday.getMonth(),bday.getDay());
    }

    public String evaluate(Integer month,Integer day){
        if(month==0){
            if(day<20){
                return "Capricorn";
            }else {
                return "Aquarius";
            }
        }
        return null;
    }

    public String evaluate(String bday){
        Date date=null;
        try {
            date=df.parse(bday);
        }catch (Exception ex){
            return null;
        }
        return this.evaluate(date.getMonth(),date.getDay());
    }
}
