package com.jiyx.test.mapred.flowStatistics;

import com.jiyx.test.mapred.flowStatistics.bo.DataBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Map
 * @author jiyx
 * @create 2018-10-15-19:42
 */
public class FlowStatisticsMapper extends Mapper<LongWritable, Text, LongWritable, DataBean> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] datas = value.toString().split("\t");
		long phoneNum = Long.parseLong(datas[1]);
		long upFlow = Long.parseLong(datas[8]);
		long downFlow = Long.parseLong(datas[9]);
		context.write(new LongWritable(phoneNum), new DataBean(phoneNum, upFlow, downFlow));
	}
}
