package com.jiyx.test.mapred.wordcount.job;

import com.jiyx.test.mapred.wordcount.mapper.WordCountMapper;
import com.jiyx.test.mapred.wordcount.reducer.WordCountReduce;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 主类
 * @author jiyx
 * @create 2018-10-13-16:01
 */
public class WordCount {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		// 构建job对象
		Job job = Job.getInstance();

		// 需要设置main方法所在的类
		job.setJarByClass(WordCount.class);

		// 设置mapper
		job.setMapperClass(WordCountMapper.class);
		// Map的输出类型设置可以省略，不过只能在reducer的输入(key,value)和输出(key,value)类型相等时才行
		// 也就是map的输出类型和reducer的输出类型一致的时候才能省略
		// 这里不相同，不能省略
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ByteWritable.class);
		// 需要统计的文件
		FileInputFormat.setInputPaths(job, new Path("/wc.txt"));

		// 设置reducer
		job.setReducerClass(WordCountReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		// 统计结果输出
		FileOutputFormat.setOutputPath(job, new Path("/wcResult"));

		// 提交任务，并打印过程信息
		job.waitForCompletion(true);
	}
}
