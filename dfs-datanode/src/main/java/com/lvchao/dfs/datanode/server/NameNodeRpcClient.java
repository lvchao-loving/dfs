package com.lvchao.dfs.datanode.server;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.lvchao.dfs.namenode.rpc.model.*;
import com.lvchao.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import org.apache.commons.lang3.StringUtils;

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
	 * 向自己负责通信的那个NameNode进行注册
	 */
	public Boolean register() throws Exception {
		ThreadUtils.println("发送RPC请求到NameNode进行注册.......");

		RegisterRequest request = RegisterRequest.newBuilder()
				.setIp(dataNodeConfig.DATANODE_IP)
				.setHostname(dataNodeConfig.DATANODE_HOSTNAME)
				.setNioPort(dataNodeConfig.NIO_PORT)
				.build();
		RegisterResponse response = namenode.register(request);

		if (response.getStatus() == 1){
			return true;
		}
		return false;
	}
	
	/**
	 * 开启发送心跳请求
	 */
	public HeartbeatResponse heartbeat() {
		HeartbeatRequest request = HeartbeatRequest.newBuilder()
				.setIp(dataNodeConfig.DATANODE_IP)
				.setHostname(dataNodeConfig.DATANODE_HOSTNAME)
				.setNioPort(dataNodeConfig.NIO_PORT)
				.build();

		return namenode.heartbeat(request);
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
	 * 上报master
	 *
	 * @param storageInfo
	 */
	public void reportCompleteStorageInfo(StorageInfo storageInfo) {
		if (storageInfo.getStoredDataSize() == 0 || storageInfo.getFilenames().size() == 0) {
			ThreadUtils.println("不需要全量上报，StorageInfo->" + JSON.toJSONString(storageInfo));
			return;
		}

		ReportCompleteStorageInfoRequest request = ReportCompleteStorageInfoRequest.newBuilder().setIp(dataNodeConfig.DATANODE_IP)
				.setHostname(dataNodeConfig.DATANODE_HOSTNAME).setFilenames(JSONArray.toJSONString(storageInfo.getFilenames()))
				.setStoredDataSize(storageInfo.getStoredDataSize()).build();
		namenode.reportCompleteStorageInfo(request);
		ThreadUtils.println("全量上报，StorageInfo->" + JSON.toJSONString(storageInfo));
	}
}
