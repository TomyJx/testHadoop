package com.jiyx.test.mapred.wordcount.reducer;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Reduce
 * @author jiyx
 * @create 2018-10-13-15:53
 */
public class WordCountReduce extends Reducer<Text, ByteWritable, Text, LongWritable> {
	@Override
	protected void reduce(Text key, Iterable<ByteWritable> values, Context context) throws IOException, InterruptedException {
		// 因为输入的是类似于{"hello": 1, 1, 1, 1}这种模式，所以直接++
		long count = 0;
		Iterator<ByteWritable> iterator = values.iterator();
		for (; iterator.hasNext(); iterator.next(), count++) {
		}
		context.write(key, new LongWritable(count));
	}
}
