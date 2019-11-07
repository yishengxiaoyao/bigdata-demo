package com.edu.bigdata.analysis.reducer;

import com.edu.bigdata.analysis.kv.key.ComDimension;
import com.edu.bigdata.analysis.kv.value.CountDurationValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountDurationReducer extends Reducer<ComDimension, Text, ComDimension, CountDurationValue> {

    private CountDurationValue countDurationValue = new CountDurationValue();

    @Override
    protected void reduce(ComDimension key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int callSum = 0;
        int callDurationSum = 0;
        for (Text text : values) {
            callSum++;
            callDurationSum += Integer.valueOf(text.toString());
        }

        countDurationValue.setCallSum(callSum);
        countDurationValue.setCallDurationSum(callDurationSum);

        context.write(key, countDurationValue);
    }
}