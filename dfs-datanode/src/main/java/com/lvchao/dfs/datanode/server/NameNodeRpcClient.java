package com.lvchao.dfs.datanode.server;

import com.lvchao.dfs.namenode.rpc.model.*;
import com.lvchao.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;

/**
 * 负责跟一组NameNode中的某一个进行通信的线程组件
 */
public class NameNodeRpcClient {
	// DataNode配置信息
	private DataNodeConfig dataNodeConfig = new DataNodeConfig();

	private NameNodeServiceGrpc.NameNodeServiceBlockingStub namenode;
	
	public NameNodeRpcClient() {
		// 初始化网络组件
		ManagedChannel channel = NettyChannelBuilder
				.forAddress(dataNodeConfig.NAMENODE_HOSTNAME, dataNodeConfig.NAMENODE_PORT)
				.negotiationType(NegotiationType.PLAINTEXT)
				.build();

		this.namenode = NameNodeServiceGrpc.newBlockingStub(channel);
		ThreadUtils.println("跟NameNode的" + dataNodeConfig.NAMENODE_PORT + "端口建立连接......");
	}

	/**
	 * 【启动注册线程】 和 【启动心跳线程】
	 * @throws Exception
	 */
	public void start() throws Exception{
		register();
		startHeartbeat();
	}

	/**
	 * 向自己负责通信的那个NameNode进行注册
	 */
	public void register() throws Exception {
		RegisterThread registerThread = new RegisterThread();
		registerThread.setName("RegisterThread");
		registerThread.start();
		// join方法将会在先执行调用join方法的线程后执行其它线程，使线程串行化
		registerThread.join();
	}
	
	/**
	 * 开启发送心跳的线程
	 */
	public void startHeartbeat() {
		HeartbeatThread heartbeatThread = new HeartbeatThread();
		heartbeatThread.setName("HeartbeatThread");
		heartbeatThread.start();
	}

	/**
	 * 通知master节点自己收到了一个文件的副本
	 * @param filename
	 */
	public void informReplicaReceived(String filename) {
		InformReplicaReceivedRequest request = InformReplicaReceivedRequest.newBuilder()
				.setFilename(filename).setIp(dataNodeConfig.DATANODE_IP).setHostname(dataNodeConfig.DATANODE_HOSTNAME).build();
		namenode.informReplicaReceived(request);
	}
	
	/**
	 * 负责注册的线程
	 */
	class RegisterThread extends Thread {
		
		@Override
		public void run() {
			try {
				ThreadUtils.println("发送RPC请求到NameNode进行注册.......");
				
				RegisterRequest request = RegisterRequest.newBuilder()
						.setIp(dataNodeConfig.DATANODE_IP)
						.setHostname(dataNodeConfig.DATANODE_HOSTNAME)
						.setNioPort(dataNodeConfig.NIO_PORT)
						.build();
				RegisterResponse response = namenode.register(request);
				ThreadUtils.println("接收到NameNode返回的注册响应：" + response.getStatus());
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
					ThreadUtils.println("每隔30秒--发送RPC请求到NameNode进行心跳.......");
					
					HeartbeatRequest request = HeartbeatRequest.newBuilder()
							.setIp(dataNodeConfig.DATANODE_IP)
							.setHostname(dataNodeConfig.DATANODE_HOSTNAME)
							.setNioPort(dataNodeConfig.NIO_PORT)
							.build();
					HeartbeatResponse response = namenode.heartbeat(request);
					ThreadUtils.println("接收到NameNode返回的心跳响应：" + response.getStatus());
					
					Thread.sleep(30 * 1000); // 每隔30秒发送一次心跳到NameNode上去
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
