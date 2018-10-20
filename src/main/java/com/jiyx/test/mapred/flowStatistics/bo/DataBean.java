package com.jiyx.test.mapred.flowStatistics.bo;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author jiyx
 * @create 2018-10-15-19:22
 */
public class DataBean implements Writable {

	private long phoneNum;

	private long upFlow;

	private long downFlow;

	private long totalFlow;

	public DataBean() {
	}

	public DataBean(long phoneNum, long upFlow, long downFlow) {
		this.phoneNum = phoneNum;
		this.downFlow = downFlow;
		this.upFlow = upFlow;
		this.totalFlow = upFlow + downFlow;
	}

	/**
	 * 序列化
	 * @param dataOutput
	 * @throws IOException
	 */
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeLong(phoneNum);
		dataOutput.writeLong(upFlow);
		dataOutput.writeLong(downFlow);
		dataOutput.writeLong(totalFlow);
	}

	/**
	 * 反序列化
	 * @param dataInput
	 * @throws IOException
	 */
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		phoneNum = dataInput.readLong();
		upFlow = dataInput.readLong();
		downFlow = dataInput.readLong();
		totalFlow = dataInput.readLong();
	}

	/**
	 * 重写toString主要是为了后面的写入文件
	 * @return
	 */
	@Override
	public String toString() {
		return this.upFlow + "\t" + this.downFlow + "\t" + this.totalFlow;
	}

	public long getPhoneNum() {
		return phoneNum;
	}

	public void setPhoneNum(long phoneNum) {
		this.phoneNum = phoneNum;
	}

	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getDownFlow() {
		return downFlow;
	}

	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}

	public long getTotalFlow() {
		return totalFlow;
	}

	public void setTotalFlow(long totalFlow) {
		this.totalFlow = totalFlow;
	}
}
