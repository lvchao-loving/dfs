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
	 * 初始化DataNode
	 */
	public DataNode() throws Exception{
		// 设置服务器启动标志
		this.shouldRun = true;

		// 创建和nameNode网络通信组件，并启动
		this.nameNodeRpcClient = new NameNodeRpcClient();

		// 发送注册请求
		Boolean registerFlag = this.nameNodeRpcClient.register();
		if (registerFlag){
			// 启动发送心跳请求（定时发送心跳）
			this.nameNodeRpcClient.startHeartbeat();

			StorageInfo storageInfo = getDataNodeStoredInfo();

			// 向NameNode发送数据
			this.nameNodeRpcClient.reportCompleteStorageInfo(storageInfo);

			// 创建上传图片线程
			DataNodeNIOServer dataNodeNIOServer = new DataNodeNIOServer(this.nameNodeRpcClient);
			dataNodeNIOServer.setName("DataNodeNIOServer");
			dataNodeNIOServer.start();
		}else {
			System.out.println("向NameNode注册失败，直接退出......");
			System.exit(1);
		}
	}

	/**
	 * 获取 DataNode 节点存储信息
	 * @return
	 */
	private StorageInfo getDataNodeStoredInfo() {
		StorageInfo storageInfo = new StorageInfo();

		File fileDir = new File(dataNodeConfig.DATA_DIR);

		scanFils(fileDir,storageInfo);

		return storageInfo;
	}

	/**
	 * 扫描遍历所有文件
	 * @param fileDir
	 * @param storageInfo
	 */
	private void scanFils(File fileDir, StorageInfo storageInfo){
		File[] fileList = fileDir.listFiles();
		for (File file:fileList) {
			if (file.isFile()){
				storageInfo.addStoredDataSize(file.length());
				storageInfo.addFilename(file.getPath().replace(dataNodeConfig.DATA_DIR,"").replace("\\","/"));
			}else {
				scanFils(file,storageInfo);
			}
		}
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
