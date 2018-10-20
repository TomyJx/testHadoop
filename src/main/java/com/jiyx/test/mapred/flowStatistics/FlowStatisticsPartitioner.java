package com.jiyx.test.mapred.flowStatistics;

import com.jiyx.test.mapred.flowStatistics.bo.DataBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * 自定义Partitioner，根据不同的前缀
 * @author jiyx
 * @create 2018-10-17-21:33
 */
public class FlowStatisticsPartitioner extends Partitioner<LongWritable, DataBean> {

	private static Map<Integer, Integer> providerMap = new HashMap<>();

	static {
		// 这只分区映射
		providerMap.put(135, 1);
		providerMap.put(136, 1);
		providerMap.put(137, 1);
		providerMap.put(138, 1);
		providerMap.put(139, 1);
		providerMap.put(150, 2);
		providerMap.put(159, 2);
		providerMap.put(182, 3);
		providerMap.put(183, 3);
	}

	/**
	 * 总共分了4个分区
	 * @param longWritable map之后的出参key
	 * @param dataBean map之后的出参value
	 * @param i 共多少个分区，设置的NumReduceTasks
	 * @return
	 */
	@Override
	public int getPartition(LongWritable longWritable, DataBean dataBean, int i) {
		int key = Integer.parseInt(String.valueOf(longWritable.get()).substring(0, 3));
		Integer code = providerMap.get(key);
		if (code == null) {
			code = 0;
		}
		return code;
	}
}
