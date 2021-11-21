package com.lvchao.dfs.namenode.server;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

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
	/**
	 * datandoe 节点组件
	 */
	private DataNodeManager dataNodeManager;
	/**
	 * checkpointTxid 文件路径
	 */
	private String checkpintTxidFilePath = "F:\\editslog\\checkpoint-txid.meta";
	/**
	 * 每个文件对应的副本所在的DataNode节点信息
	 */
	private Map<String,List<DataNodeInfo>> replicasByFilename = new HashMap<>();

	/**
	 * 构造函数，初始化组件
	 * @param dataNodeManager
	 */
	public FSNamesystem(DataNodeManager dataNodeManager) {
		this.directory = new FSDirectory();
		this.editlog = new FSEditlog(this);
		this.dataNodeManager = dataNodeManager;
		// 加载磁盘 fsimage 文件元数据
		recoverNamespace();
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
	 * 创建文件
	 * @param filename
	 * @return
	 * @throws Exception
	 */
	public Boolean create(String filename) throws Exception{
		if(!directory.create(filename)) {
			return false;
		}
		editlog.logEdit(EditLogFactory.create(filename));
		return true;
	}

	/**
	 * 将 checkpointTxid 保存到磁盘中
	 */
	public void saveCheckPointTxid(){
		try {
			// 先把上一次的fsimage文件删除
			String filePath = checkpintTxidFilePath;
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

	/**
	 * 恢复元数据
	 */
	public void recoverNamespace() {
		// 加载 BackupNode 节点发送过来的fsimage 文件直接记载到内存中
		loadFSImage();
		// 加载 checkpointTxid 到内存中
		loadCheckpointTxid();
		// 加载 editslog 日志文件结合 checkpointTxid
		loadEditsLog();
	}

	/**
	 *  加载磁盘中的 editslog 日志文件恢复到文件目录树中
	 */
	private void loadEditsLog() {
		try {
			String fileDirectory = "F:\\editslog";
			String fileNamePrefix = "edits-";
			File fileAll = new File(fileDirectory);

			List<File> fileList = new ArrayList<>();

			// 收集需要排序的文件
			for (File file:fileAll.listFiles()){
				if (file.isFile() && file.getName().contains(fileNamePrefix)){
					fileList.add(file);
				}
			}
			if (fileList.size() == 0){
				ThreadUntils.println("无 fsimage 文件不需要文件恢复");
				return;
			}

			// 根据文件名称排序
			Collections.sort(fileList, new Comparator<File>() {
				@Override
				public int compare(File o1, File o2) {
					Integer o1Sequence = Integer.valueOf(o1.getName().split("-")[1]);
					Integer o2Sequence = Integer.valueOf(o2.getName().split("-")[1]);
					return o1Sequence - o2Sequence;
				}
			});

			for (File file:fileList){
				List<String> fileLineContent = Files.readAllLines(Paths.get(file.getPath()), StandardCharsets.UTF_8);
				for (String lineContent:fileLineContent){
					JSONObject jsonObject = JSONObject.parseObject(lineContent);
					Long txid = jsonObject.getLongValue("txid");
					if (txid > checkpointTxid){
						System.out.println("准备回放数据：" + lineContent);
						// 回放数据到内存中
						String op = jsonObject.getString("OP");
						if ("MKDIR".equals(op)){
							directory.mkdir(jsonObject.getString("PATH"));
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 加载持久化磁盘中的 checkpointTxid
	 */
	private void loadCheckpointTxid() {
		try {
			// 先把上一次的fsimage文件删除
			String filePath = checkpintTxidFilePath;
			File file = new File(filePath);
			if(!file.exists()) {
				ThreadUntils.println("启动恢复checkpointTxid 文件不存在!");
				return;
			}
			try (
					RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
					FileInputStream fileInputStream = new FileInputStream(randomAccessFile.getFD());
					FileChannel fileChannel = fileInputStream.getChannel();
			){
				ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
				StringBuilder message = new StringBuilder();
				while (true) {
					if (fileChannel.read(byteBuffer) <= 0) {
						ThreadUntils.println("退出读取文件循环");
						break;
					}
					byteBuffer.flip();
					message.append(StandardCharsets.UTF_8.decode(byteBuffer).toString());
					byteBuffer.clear();
				}
				this.checkpointTxid = Long.valueOf(StringUtils.isBlank(message.toString())? "0" : message.toString());
			} catch (Exception e){
				e.printStackTrace();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 加载 fsimage 文件到内存里进行恢复
	 */
	public void loadFSImage(){
		String path = "F:\\editslog\\fsimage.meta";
		File file = new File(path);
		// 文件不存在则说明不需要恢复
		if (!file.exists()){
			ThreadUntils.println("fsimage文件不存在，不需要进行恢复");
			return;
		}
		try (
			FileInputStream fileInputStream = new FileInputStream(path);
			FileChannel fileChannel = fileInputStream.getChannel();
		){
			ByteBuffer byteBuffer = ByteBuffer.allocate(1024 * 1024);
			StringBuilder fsimageSB = new StringBuilder();
			while (true) {
				if (fileChannel.read(byteBuffer) <= 0) {
					ThreadUntils.println("退出读取文件循环");
					break;
				}
				byteBuffer.flip();
				fsimageSB.append(StandardCharsets.UTF_8.decode(byteBuffer).toString());
				byteBuffer.clear();
			}
			FSDirectory.INode iNode = JSONObject.parseObject(fsimageSB.toString(), FSDirectory.INode.class);
			directory.setDirTree(iNode);
		}catch (Exception e){
			e.printStackTrace();
		}
	}

	public String getCheckpintTxidFilePath() {
		return checkpintTxidFilePath;
	}

	public void setCheckpintTxidFilePath(String checkpintTxidFilePath) {
		this.checkpintTxidFilePath = checkpintTxidFilePath;
	}


	/**
	 * 给指定的文件增加一个成功接收的文件副本
	 * @param hostname
	 * @param ip
	 * @param filename
	 * @throws Exception
	 */
	public void addReceivedReplica(String hostname, String ip, String filename) throws Exception{
		synchronized (replicasByFilename){
			List<DataNodeInfo> dataNodeInfoList = replicasByFilename.get(filename);

			if (dataNodeInfoList == null){
				dataNodeInfoList = new ArrayList<DataNodeInfo>();
				replicasByFilename.put(filename,dataNodeInfoList);
			}

			DataNodeInfo dataNodeInfo = dataNodeManager.getDataNodeInfo(ip, hostname);

			dataNodeInfoList.add(dataNodeInfo);
		}
	}
}
