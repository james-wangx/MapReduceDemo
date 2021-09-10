package com.pineapple.mapreduce.reduceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Reducer<Text, TableBean, TableBean, NullWritable>.
            Context context) throws IOException, InterruptedException {
        // 创建一个集合用于存放 order 表的数据
        ArrayList<TableBean> orderBeans = new ArrayList<>();
        // 存放 pd 表的数据
        TableBean pdBean = new TableBean();

        for (TableBean value : values) {
            if ("order".equals(value.getFlag())) {
                TableBean tempTableBean = new TableBean();

                // 属性赋值
                try {
                    BeanUtils.copyProperties(tempTableBean, value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }

                orderBeans.add(tempTableBean);
            } else {
                try {
                    BeanUtils.copyProperties(pdBean, value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        // 遍历赋值
        for (TableBean orderBean : orderBeans) {
            orderBean.setPName(pdBean.getPName());
            context.write(orderBean, NullWritable.get());
        }
    }
}
