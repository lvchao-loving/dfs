package com.lvchao.dfs.datanode.server;

import java.io.File;

/**
 * DataNode启动类
 */
public class DataNode {

	/**
	 * 初始化文件配置
	 */
	private DataNodeConfig dataNodeConfig = new DataNodeConfig();

	/**
	 * 是否还在运行
	 */
	private volatile Boolean shouldRun;

	/**
	 * 负责跟一组NameNode通信的组件
	 */
	private NameNodeRpcClient nameNodeRpcClient;
	/**
	 * 心跳发送组件
	 */
	private HeartbeatManager heartbeatManager;
	/**
	 * 文件磁盘管理器
	 */
	private StorageManager storageManager;
	/**
	 * 复制任务管理组件
	 */
	private ReplicateManager replicateManager;


	/**
	 * 初始化DataNode
	 */
	public DataNode() throws Exception{
		// 设置服务器启动标志
		this.shouldRun = true;

		// 创建和nameNode网络通信组件，并启动
		this.nameNodeRpcClient = new NameNodeRpcClient();
		this.storageManager = new StorageManager();
		// 发送注册请求-并更具注册请求处理
		Boolean registerFlag = this.nameNodeRpcClient.register();
		if (registerFlag){
			StorageInfo storageInfo = this.storageManager.getDataNodeStoredInfo();
			this.nameNodeRpcClient.reportCompleteStorageInfo(storageInfo);
		}else {
			ThreadUtils.println("不需要全量上报存储信息...");
		}
		this.replicateManager = new ReplicateManager(this.nameNodeRpcClient);

		this.heartbeatManager = new HeartbeatManager(this.nameNodeRpcClient, this.storageManager, this.replicateManager);
		this.heartbeatManager.start();

		// 创建上传图片线程
		DataNodeNIOServer dataNodeNIOServer = new DataNodeNIOServer(this.nameNodeRpcClient);
		dataNodeNIOServer.setName("DataNodeNIOServer");
		dataNodeNIOServer.start();

	}

	/**
	 * 运行 DataNode TODO
	 */
	private void start() {
		try {
			while(shouldRun) {
				Thread.sleep(1000);  
			}   
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception{
		DataNode datanode = new DataNode();
		datanode.start();
	}
}
