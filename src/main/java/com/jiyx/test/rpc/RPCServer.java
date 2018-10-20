package com.jiyx.test.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * @author jiyx
 * @create 2018-09-27-18:45
 */
public class RPCServer implements Bizable {

	@Override
	public String hello(String name) {
		return "hello " + name;
	}

	public static void main(String[] args) throws IOException {

		RPC.Server server = new RPC.Builder(new Configuration())// config是必传的，这里虽然只是new了对象没做任何操作，但是构造器中有很多配置
				.setBindAddress("172.18.3.82")// 设置服务端的ip
				.setInstance(new RPCServer())// 设置服务端的实例
				.setProtocol(Bizable.class)// 服务端的接口
				.setPort(8888)// 端口
				.build();// 创建服务实例
		server.start();
	}
}
