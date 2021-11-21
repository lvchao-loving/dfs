package com.lvchao.dfs.datanode.server;

/**
 * DataNode启动类
 */
public class DataNode {

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
		this.nameNodeRpcClient.start();

		// 创建上传图片线程
		DataNodeNIOServer dataNodeNIOServer = new DataNodeNIOServer(nameNodeRpcClient);
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
