package com.jiyx.test.mapred.sort;

import com.jiyx.test.mapred.sort.DataBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * key是自定义javaBean，只需要实现了WritableComparable接口，
 * 那么shuffle在进行排序的时候就会按照用户自动的方法进行排序了
 * @author jiyx
 * @create 2018-10-20-16:04
 */
public class SortMapper extends Mapper<LongWritable, Text, DataBean, NullWritable> {

	private DataBean k = new DataBean();

	@Override
	protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
		String[] datas = value.toString().split("\t");
		long phoneNum = Long.parseLong(datas[0]);
		long upFlow = Long.parseLong(datas[1]);
		long downFlow = Long.parseLong(datas[2]);
		context.write(k.set(phoneNum, upFlow, downFlow), NullWritable.get());
	}
}
