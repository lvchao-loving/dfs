package com.lvchao.dfs.namenode.server;

import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

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

	/**
	 * 将 checkpointTxid 保存到磁盘中
	 */
	public void saveCheckPointTxid(){
		try {
			// 先把上一次的fsimage文件删除
			String filePath = "F:\\editslog\\checkpoint-txid.meta";
			File file = new File(filePath);
			if(file.exists()) {
				file.delete();
			}
			try (
					RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
					FileOutputStream fileOutputStream = new FileOutputStream(randomAccessFile.getFD());
					FileChannel fileChannel = fileOutputStream.getChannel();
			){
				fileChannel.write(StandardCharsets.UTF_8.encode(String.valueOf(checkpointTxid)));
				fileChannel.force(true);
				ThreadUntils.println("保存checkpointTxid:" + checkpointTxid);
			} catch (Exception e){
				e.printStackTrace();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
