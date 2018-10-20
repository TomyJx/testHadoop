package com.jiyx.test.mapred.flowStatistics;

import com.jiyx.test.mapred.flowStatistics.bo.DataBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reduce
 * @author jiyx
 * @create 2018-10-15-19:54
 */
public class FlowStatisticsReducer extends Reducer<LongWritable, DataBean, LongWritable, DataBean> {
	@Override
	protected void reduce(LongWritable key, Iterable<DataBean> values, Context context) throws IOException, InterruptedException {
		long upFlowSum = 0;
		long downFlowSum = 0;
		for (DataBean value : values) {
			upFlowSum += value.getUpFlow();
			downFlowSum += value.getDownFlow();
		}
		context.write(key, new DataBean(key.get(), upFlowSum, downFlowSum));
	}
}
