package com.jiyx.test.mapred.wordcount.mapper;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Map
 * @author jiyx
 * @create 2018-10-13-15:42
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, ByteWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 对输入的value进行拆分后直接输出,如{18:"hello java hello"}，输出为"hello",1和"java",1和"hello",1
		for (String newKey : value.toString().split(" ")) {
			context.write(new Text(newKey), new ByteWritable((byte) 1));
		}
	}
}
