package com.jiyx.test.mapred.sort;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author jiyx
 * @create 2018-10-20-16:35
 */
public class SortJob {
	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance();

		job.setJarByClass(SortJob.class);

		job.setMapperClass(SortMapper.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		job.setReducerClass(SortReducer.class);
		// 这块需要注意的是自己踩了一个坑，就是将key和value整反了
		// 然后就会出现异常java.io.IOException: Initialization of all the collectors failed. Error in last collector was:java.lang.ClassCastException: class com.jiyx.test.mapred.flowStatistics.bo.DataBean
		// 所以这里最好注意下
		job.setOutputKeyClass(DataBean.class);
		job.setOutputValueClass(NullWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
