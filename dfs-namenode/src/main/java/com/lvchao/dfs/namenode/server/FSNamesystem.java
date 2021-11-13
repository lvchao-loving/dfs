package com.lvchao.dfs.namenode.server;

/**
 * 负责管理元数据的核心组件
 */
public class FSNamesystem {

	/**
	 * 负责管理内存文件目录树的组件
	 */
	private FSDirectory directory;
	/**
	 * 负责管理edits log写入磁盘的组件
	 */
	private FSEditlog editlog;
	/**
	 * 最近一次checkpoint更新到的txid，初始化id
	 */
	private Long checkpointTxid = 0L;
	
	public FSNamesystem() {
		this.directory = new FSDirectory();
		this.editlog = new FSEditlog(this);
	}
	
	/**
	 * 创建目录
	 * @param path 目录路径
	 * @return 是否成功
	 */
	public Boolean mkdir(String path) throws Exception {
		this.directory.mkdir(path); 
		this.editlog.logEdit("{'OP':'MKDIR','PATH':'" + path + "'}");
		return true;
	}

	/**
	 * 强制将内存中的数据刷新到磁盘
	 */
	public void flush(){
		this.editlog.flush();
	}

	/**
	 * 获取 FSEditlog 组件
	 * @return
	 */
	public FSEditlog getFSEditlog(){
		return editlog;
	}

	public Long getCheckpointTxid() {
		return checkpointTxid;
	}

	public void setCheckpointTxid(Long checkpointTxid) {
		ThreadUntils.println("接收到的checkpointTxid:" + checkpointTxid);
		this.checkpointTxid = checkpointTxid;
	}
}
