package com.jiyx.test.mapred.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author jiyx
 * @create 2018-10-20-16:19
 */
public class SortReducer extends Reducer<DataBean, NullWritable, DataBean, NullWritable> {
	@Override
	protected void reduce(DataBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		context.write(key, NullWritable.get());
	}
}
