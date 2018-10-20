package com.jiyx.test.mapred.flowStatistics;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 季节分区
 * @author jiyx
 * @create 2018-10-20-13:40
 */
public class MonthPartitoner extends Partitioner<ByteWritable, Text> {
	@Override
	public int getPartition(ByteWritable byteWritable, Text text, int i) {
		return getSeason(byteWritable.get());
	}

	private int getSeason(byte month){
		switch (month) {
			case 4 :
				// "春季"
				return 1;
			case 5 :
			case 6 :
			case 7 :
			case 8 :
			case 9 :
				// "夏季"
				return 2;
			case 10 :
				// "秋季"
				return 3;
			default:
				// "冬季"
				return 4;
		}
	}
}
