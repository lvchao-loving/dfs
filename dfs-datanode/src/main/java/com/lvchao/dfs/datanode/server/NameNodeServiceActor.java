package com.lvchao.dfs.datanode.server;

import com.lvchao.dfs.namenode.rpc.model.HeartbeatRequest;
import com.lvchao.dfs.namenode.rpc.model.HeartbeatResponse;
import com.lvchao.dfs.namenode.rpc.model.RegisterRequest;
import com.lvchao.dfs.namenode.rpc.model.RegisterResponse;
import com.lvchao.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import static com.lvchao.dfs.datanode.server.DataNodeConfig.*;

/**
 * 负责跟一组NameNode中的某一个进行通信的线程组件
 */
public class NameNodeServiceActor {

	private NameNodeServiceGrpc.NameNodeServiceBlockingStub namenode;
	
	public NameNodeServiceActor() {
		// 初始化网络组件
		ManagedChannel channel = NettyChannelBuilder
				.forAddress(NAMENODE_HOSTNAME, NAMENODE_PORT)
				.negotiationType(NegotiationType.PLAINTEXT)
				.build();
		this.namenode = NameNodeServiceGrpc.newBlockingStub(channel);
	}

	/**
	 * 向自己负责通信的那个NameNode进行注册
	 */
	public void register() throws Exception {
		Thread registerThread = new RegisterThread();
		registerThread.start();
		// join方法将会在先执行调用join方法的线程后执行其它线程，使线程串行化
		registerThread.join();
	}
	
	/**
	 * 开启发送心跳的线程
	 */
	public void startHeartbeat() {
		new HeartbeatThread().start();
	}
	
	/**
	 * 负责注册的线程
	 */
	class RegisterThread extends Thread {
		
		@Override
		public void run() {
			try {
				ThreadUntils.println("发送RPC请求到NameNode进行注册.......");
				
				RegisterRequest request = RegisterRequest.newBuilder()
						.setIp(DATANODE_IP)
						.setHostname(DATANODE_HOSTNAME)
						.setNioPort(NIO_PORT)
						.build();
				RegisterResponse response = namenode.register(request);
				ThreadUntils.println("接收到NameNode返回的注册响应：" + response.getStatus());  
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
	
	/**
	 * 负责心跳的线程
	 */
	class HeartbeatThread extends Thread {
		
		@Override
		public void run() {
			try {
				while(true) {
					ThreadUntils.println("发送RPC请求到NameNode进行心跳.......");  
					
					HeartbeatRequest request = HeartbeatRequest.newBuilder()
							.setIp(DATANODE_IP)
							.setHostname(DATANODE_HOSTNAME)
							.setNioPort(NIO_PORT)
							.build();
					HeartbeatResponse response = namenode.heartbeat(request);
					ThreadUntils.println("接收到NameNode返回的心跳响应：" + response.getStatus());  
					
					Thread.sleep(5 * 1000); // 每隔30秒发送一次心跳到NameNode上去
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
	
}
