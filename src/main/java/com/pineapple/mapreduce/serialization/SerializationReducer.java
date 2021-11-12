package com.pineapple.mapreduce.serialization;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SerializationReducer extends Reducer<Text, SerializationBean, SerializationBean, NullWritable> {

    private final SerializationBean bean = new SerializationBean();

    @Override
    protected void reduce(Text key, Iterable<SerializationBean> values,
            Reducer<Text, SerializationBean, SerializationBean, NullWritable>.Context context)
            throws IOException, InterruptedException {
        int totalUp = 0;
        int totalDown = 0;

        for (SerializationBean value : values) {
            totalUp += value.getUpFlow();
            totalDown += value.getDownFlow();
        }

        bean.setPhone(key.toString());
        bean.setUpFlow(totalUp);
        bean.setDownFlow(totalDown);
        bean.setSumFlow();

        context.write(bean, NullWritable.get());
    }
}
