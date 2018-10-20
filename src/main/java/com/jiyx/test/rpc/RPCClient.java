package com.jiyx.test.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolProxy;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author jiyx
 * @create 2018-09-27-18:45
 */
public class RPCClient {
	public static void main(String[] args) throws IOException {
		// 调用的时候，有一个clientVersion
		// 但是这个clientVersion并不一定要和服务端接口中的id一致，
		// 而早期版本是需要一致的
		ProtocolProxy<Bizable> proxy = RPC.getProtocolProxy(
				Bizable.class, //
				1000, // 客户端版本号
				new InetSocketAddress("172.18.3.82", 8888), // 服务的端口和ip
				new Configuration()// 配置信息
		);
		Bizable bizable = proxy.getProxy();
		// 接口调用
		String hello = bizable.hello("jiyx");
		System.out.println(hello);
	}
}
