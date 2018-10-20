package com.jiyx.test.mapred.flowStatistics;

import com.jiyx.test.mapred.flowStatistics.bo.DataBean;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Job
 * @author jiyx
 * @create 2018-10-15-19:21
 */
public class FlowStatistics {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance();

		job.setJarByClass(FlowStatistics.class);

		job.setMapperClass(FlowStatisticsMapper.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		job.setReducerClass(FlowStatisticsReducer.class);
		// 这块需要注意的是自己踩了一个坑，就是将key和value整反了
		// 然后就会出现异常java.io.IOException: Initialization of all the collectors failed. Error in last collector was:java.lang.ClassCastException: class com.jiyx.test.mapred.flowStatistics.bo.DataBean
		// 所以这里最好注意下
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(DataBean.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// partitioner
		job.setPartitionerClass(FlowStatisticsPartitioner.class);
		// 这个指定的数值必须大于等于分区数，1除外。指定1的话就是默认的伪集群分区。
		// 除了1以外，不能比FlowStatisticsPartitioner中划分的分区数量少
		// 如本例中分了4个分区(从0到3共4个)，那么这里指定为2的话，只有前两个分区的数据知道去哪个reduce处理数据
		// 后两个不知道去哪个reduce处理，会报错，但是这里测试了下，有中情况是不会报错的
		// 就是当我的数据只有前两个的分区数据时，也就是虽然我分了4个分区，
		// 但是实际只会产生两个分区的数据，就不会报错，不知道算不算bug
		job.setNumReduceTasks(Integer.parseInt(args[2]));

		// combiner
		job.setCombinerClass(FlowStatisticsReducer.class);

		job.waitForCompletion(true);
	}
}
