package com.lvchao.dfs.namenode.server;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.lvchao.dfs.namenode.rpc.model.*;
import com.lvchao.dfs.namenode.rpc.service.*;

import io.grpc.stub.StreamObserver;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * NameNode的rpc服务的接口
 */
public class NameNodeServiceImpl implements NameNodeServiceGrpc.NameNodeService {

	public static final Integer STATUS_SUCCESS = 1;
	public static final Integer STATUS_FAILURE = 2;
	public static final Integer STATUS_SHUTDOWN = 3;
	public static final Integer STATUS_DUPLICATE = 4;

	/**
	 * BackupNode 节点拉取数据的大小
	 */
	public static final Integer BACKUP_NODE_FETCH_SIZE = 10;
	/**
	 * 负责管理元数据的核心组件
	 */
	private FSNamesystem namesystem;
	/**
	 * 负责管理集群中所有的datanode的组件
	 */
	private DataNodeManager datanodeManager;
	/**
	 * 是否还在运行，当isRunning为false则表明当前没有不允许其它操作
	 */
	private volatile Boolean isRunning = true;
	/**
	 * 当前缓冲的一小部分 editslog
	 */
	private JSONArray currentBufferedEditsLog = new JSONArray();
	/**
	 * 当前缓存里的 editslog 最大的一个txid
	 */
	private Long currentBufferedMaxTxid = 0L;
	/**
	 * 当前内存里缓冲了哪个磁盘文件的数据
	 */
	private String bufferedFlushedTxid;

	public NameNodeServiceImpl(
			FSNamesystem namesystem, 
			DataNodeManager datanodeManager) {
		this.namesystem = namesystem;
		this.datanodeManager = datanodeManager;
	}

	/**
	 * datanode进行注册
	 * @param request
	 * @param responseObserver
	 */
	@Override
	public void register(RegisterRequest request, 
			StreamObserver<RegisterResponse> responseObserver) {
		// 注册服务
		Boolean registerFlag = datanodeManager.register(request.getIp(), request.getHostname(), request.getNioPort());

		RegisterResponse response = null;
		if (registerFlag){
			response = RegisterResponse.newBuilder().setStatus(STATUS_SUCCESS).build();
		}else{
			response = RegisterResponse.newBuilder().setStatus(STATUS_FAILURE).build();
		}
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	/**
	 * datanode进行心跳
	 * @param request
	 * @param responseObserver
	 */
	@Override
	public void heartbeat(HeartbeatRequest request, 
			StreamObserver<HeartbeatResponse> responseObserver) {
		// 接收请求完成注册
		String ip = request.getIp();
		String hostname = request.getHostname();
		Boolean heartbeatResult = datanodeManager.heartbeat(ip, hostname);

		HeartbeatResponse response = null;

		List<Command> commandList = new ArrayList<>();

		if (heartbeatResult){
			// 处理 DataNodeInfo 节点中任务队列的数据
			List<Command> commandAddList = handleReplicateTaskQueue(ip, hostname);
			if (commandAddList.size() > 0){
				commandList.addAll(commandAddList);
			}

			// 处理 DataNodeInfo 节点中任务队列的数据
			List<Command> commandRemoveList = handleRemoveReplicateTaskQueue(ip, hostname);
			if (commandRemoveList.size() > 0){
				commandList.addAll(commandRemoveList);
			}

			response = HeartbeatResponse.newBuilder()
					.setStatus(STATUS_SUCCESS)
					.setCommands(JSONArray.toJSONString(commandList))
					.build();
		}else { // 注册失败处理的逻辑

			// 封装注册的命令
			Command commandREGISTER = new Command(Command.REGISTER);
			// 封装全量上报的命令
			Command commandREPORT_COMPLETE_STORAGE_INFO = new Command(Command.REPORT_COMPLETE_STORAGE_INFO);
			commandList.add(commandREGISTER);
			commandList.add(commandREPORT_COMPLETE_STORAGE_INFO);
			response = HeartbeatResponse.newBuilder()
					.setStatus(STATUS_FAILURE)
					.setCommands(JSONArray.toJSONString(commandList))
					.build();
		}
	
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	private List<Command> handleRemoveReplicateTaskQueue(String ip, String hostname) {
		DataNodeInfo dataNodeInfo = datanodeManager.getDataNodeInfo(ip, hostname);
		List<Command> commandList = new ArrayList<>();
		while (true){
			RemoveReplicaTask removeReplicaTask = dataNodeInfo.pollRemoveReplicaTask();
			if (Objects.isNull(removeReplicaTask)){
				break;
			}
			Command command = new Command(Command.REMOVE_REPLICA);
			command.setContent(JSONObject.toJSONString(removeReplicaTask));
			commandList.add(command);
		}
		return commandList;
	}

	/**
	 * 处理 DataNodeInfo 节点中任务队列的数据
	 * @param ip
	 * @param hostname
	 * @return
	 */
	private List<Command> handleReplicateTaskQueue(String ip, String hostname) {
		DataNodeInfo dataNodeInfo = datanodeManager.getDataNodeInfo(ip, hostname);
		List<Command> commandList = new ArrayList<>();
		while (true){
			ReplicateTask replicateTask = dataNodeInfo.pollReplicateTask();
			if (Objects.isNull(replicateTask)){
				break;
			}
			Command command = new Command(Command.REPLICATE);
			command.setContent(JSONObject.toJSONString(replicateTask));
			commandList.add(command);
		}
		return commandList;
	}

	@Override
	public void mkdir(MkdirRequest request, StreamObserver<MkdirResponse> responseObserver) {
		try {
			MkdirResponse response = null;
			if (!isRunning){
				response = MkdirResponse.newBuilder().setStatus(STATUS_SHUTDOWN).build();
			}else {
				this.namesystem.mkdir(request.getPath());
				response = MkdirResponse.newBuilder()
						.setStatus(STATUS_SUCCESS)
						.build();
			}
			responseObserver.onNext(response);
			responseObserver.onCompleted();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void shutdown(ShutdownRequest request, StreamObserver<ShutdownResponse> responseObserver) {
		try {
			// 关闭 NameNode 服务标识
			this.isRunning = false;
			// 将内存中的 editslog 持久化到磁盘中
			this.namesystem.flush();
			// 将内存中的 CheckPointTxid 持久化到磁盘中
			this.namesystem.saveCheckPointTxid();

			ShutdownResponse response = ShutdownResponse.newBuilder().setStatus(STATUS_SUCCESS).build();
			responseObserver.onNext(response);
			responseObserver.onCompleted();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void fetchEditsLog(FetchEditsLogRequest request, StreamObserver<FetchEditsLogResponse> responseObserver) {
		// true：NameNode节点正在关闭
		if (!isRunning){
			FetchEditsLogResponse fetchEditsLogResponse = FetchEditsLogResponse.newBuilder().setEditsLog(new JSONArray().toJSONString()).build();
			responseObserver.onNext(fetchEditsLogResponse);
			responseObserver.onCompleted();
			return;
		}

		Long syncedTxid = request.getSyncedTxid();

		FetchEditsLogResponse response = null;
		// 记录当前 BackupNode 节点拉取数据
		JSONArray fetchedEditsLog = new JSONArray();

		// 获取持久磁盘的文件 txid 记录
		List<String> flushedTxids = namesystem.getFSEditlog().getFlushedTxids();

		// NameNode-没有同步过磁盘，如果有数据的话也仅仅在内存中
		if(flushedTxids.size() == 0) {
			ThreadUtils.println("暂时没有任何磁盘文件，直接从内存缓冲中拉取editslog......");
			fetchFromBufferedEditsLog(syncedTxid, fetchedEditsLog);
		}
		// NameNode-同步过磁盘，从磁盘中拉取数据
		else {
			// 有磁盘文件，缓存中有存储过磁盘文件
			if(bufferedFlushedTxid != null) {
				if (existInFlushedFile(syncedTxid, bufferedFlushedTxid)){
					ThreadUtils.println("上一次已经缓存过磁盘文件的数据，直接从磁盘文件缓存中拉取editslog......");
					fetchFromCurrentBuffer(syncedTxid, fetchedEditsLog);
				}
				// 拉取的数据不再当前缓存的配置文件中，需要从下一个磁盘文件中拉取
				else {
					String nextFlushedTxid = getNextFlushedTxid(flushedTxids, bufferedFlushedTxid);
					// 如果可以找到下一个磁盘文件，那么就从下一个磁盘文件里开始读取数据
					if(nextFlushedTxid != null) {
						ThreadUtils.println("上一次缓存的磁盘文件找不到要拉取的数据，从下一个磁盘文件中拉取editslog......");
						fetchFromFlushedFile(syncedTxid,nextFlushedTxid, fetchedEditsLog);
					}
					// 如果没有找到下一个文件，此时就需要从内存里去继续读取
					else {
						ThreadUtils.println("上一次缓存的磁盘文件找不到要拉取的数据，而且没有下一个磁盘文件，尝试从内存缓冲中拉取editslog......");
						fetchFromBufferedEditsLog(syncedTxid, fetchedEditsLog);
					}
				}
			}
			// 有磁盘文件，缓存中没有存储过磁盘文件
			else {
				// 标志位
				Boolean fetchedFromFlushedFile = false;
				// 遍历所有持久磁盘的文件
				for (String flushedTxid:flushedTxids) {
					// 判断需要下次拉取的 txid 是否在当前缓存文件中
					if (existInFlushedFile(syncedTxid, flushedTxid)){
						ThreadUtils.println("尝试从磁盘文件中拉取editslog，flushedTxid=" + flushedTxid);
						// 将对应的磁盘文件加载到内存中
						fetchFromFlushedFile(syncedTxid, flushedTxid, fetchedEditsLog);
						fetchedFromFlushedFile = true;
						break;
					}
				}

				// 遍历所有文件没有找到则从缓存中去继续读取
				if(!fetchedFromFlushedFile){
					ThreadUtils.println("所有磁盘文件都没找到要拉取的editslog，尝试直接从内存缓冲中拉取editslog......");
					fetchFromBufferedEditsLog(syncedTxid, fetchedEditsLog);
				}
			}
		}

		// 封装返回值请求
		response = FetchEditsLogResponse.newBuilder()
				.setEditsLog(fetchedEditsLog.toJSONString())
				.build();

		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	/**
	 * 更新checkpointd的txid
	 * @param request
	 * @param responseObserver
	 */
	@Override
	public void updateCheckpointTxid(UpdateCheckpointTxidRequest request, StreamObserver<UpdateCheckpointTxidResponse> responseObserver) {
		try {
			// 从请求中获取 checkpointTxid
			long txid = request.getTxid();
			namesystem.setCheckpointTxid(txid);
			UpdateCheckpointTxidResponse response = UpdateCheckpointTxidResponse.newBuilder().setStatus(STATUS_SUCCESS).build();
			responseObserver.onNext(response);
			responseObserver.onCompleted();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void create(CreateFileRequest request, StreamObserver<CreateFileResponse> responseObserver) {
		try {
			CreateFileResponse response = null;

			if(!isRunning) {
				response = CreateFileResponse.newBuilder()
						.setStatus(STATUS_SHUTDOWN)
						.build();
			} else {
				String filename = request.getFilename();
				Boolean success = namesystem.create(filename);

				if(success) {
					response = CreateFileResponse.newBuilder()
							.setStatus(STATUS_SUCCESS)
							.build();
				} else {
					response = CreateFileResponse.newBuilder()
							.setStatus(STATUS_DUPLICATE)
							.build();
				}
			}

			responseObserver.onNext(response);
			responseObserver.onCompleted();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void allocateDataNodes(AllocateDataNodesRequest request, StreamObserver<AllocateDataNodesResponse> responseObserver) {
		Long fileSize = request.getFileSize();
		List<DataNodeInfo> datanodes = datanodeManager.allocateDataNodes(fileSize);
		String datanodesJson = JSONArray.toJSONString(datanodes);

		AllocateDataNodesResponse response = AllocateDataNodesResponse.newBuilder()
				.setDatanodes(datanodesJson)
				.build();

		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	@Override
	public void informReplicaReceived(InformReplicaReceivedRequest request, StreamObserver<InformReplicaReceivedResponse> responseObserver) {
		try {
			InformReplicaReceivedResponse response = null;
			if (!isRunning){
				response = InformReplicaReceivedResponse.newBuilder().setStatus(STATUS_SHUTDOWN).build();
			}else {
				String filename = request.getFilename();
				namesystem.addReceivedReplica(request.getHostname(),request.getIp(), filename.split("_")[0], Long.valueOf(filename.split("_")[1]));
				response = InformReplicaReceivedResponse.newBuilder()
						.setStatus(STATUS_SUCCESS)
						.build();
			}
			responseObserver.onNext(response);
			responseObserver.onCompleted();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void reportCompleteStorageInfo(ReportCompleteStorageInfoRequest request, StreamObserver<ReportCompleteStorageInfoResponse> responseObserver) {
		try {
			ReportCompleteStorageInfoResponse response = null;
			if (!isRunning){
				response = ReportCompleteStorageInfoResponse.newBuilder().setStatus(STATUS_SHUTDOWN).build();
			}else {
				// 将发送过来 storedDataSize 设置到 DataNodeManager 组件中
				datanodeManager.setStoredDataSize(request.getIp(),request.getHostname(),request.getStoredDataSize());

				JSONArray filenames = JSONArray.parseArray(request.getFilenames());

				for (int i = 0; i < filenames.size(); i++) {
					String filename = filenames.getString(i);
					namesystem.addReceivedReplica(request.getHostname(), request.getIp(), filename.split("_")[0], Long.valueOf(filename.split("_")[1]));
				}

				response = ReportCompleteStorageInfoResponse.newBuilder()
						.setStatus(STATUS_SUCCESS)
						.build();
			}
			responseObserver.onNext(response);
			responseObserver.onCompleted();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 描述：通过 filename 发起请求，返回 file 对应存储的 DataNode
	 * @param request
	 * @param responseObserver
	 */
	@Override
	public void getDataNodeForFile(GetDataNodeForFileRequest request, StreamObserver<GetDataNodeForFileResponse> responseObserver) {
		try {
			String filename = request.getFilename();
			DataNodeInfo dataNodeInfo = namesystem.getDataNodeForFile(filename);
			GetDataNodeForFileResponse response = GetDataNodeForFileResponse.newBuilder()
					.setDatanodeInfo(JSON.toJSONString(dataNodeInfo)).build();
			responseObserver.onNext(response);
			responseObserver.onCompleted();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 从已经刷入磁盘的文件里读取 edislog，同时缓存找个文件数据到内存
	 * @param syncedTxid
	 * @param flushedTxid
	 * @param fetchedEditsLog
	 */
	private void fetchFromFlushedFile(Long syncedTxid, String flushedTxid, JSONArray fetchedEditsLog){
		try {
			String[] flushedTxidSplited = flushedTxid.split("_");

			long startTxid = Long.valueOf(flushedTxidSplited[0]);
			long endTxid = Long.valueOf(flushedTxidSplited[1]);

			String currentEditsLogFile = "F:\\editslog\\edits_" + startTxid + "_" + endTxid + ".log";

			List<String> editsLogs = Files.readAllLines(Paths.get(currentEditsLogFile));
			for(String editsLog : editsLogs) {
				currentBufferedEditsLog.add(JSONObject.parseObject(editsLog));
				currentBufferedMaxTxid = JSONObject.parseObject(editsLog).getLongValue("txid");
			}
			// 缓存记录正在同步的文件名称
			bufferedFlushedTxid = flushedTxid;

			fetchFromCurrentBuffer(syncedTxid,fetchedEditsLog);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 是否存在于刷到磁盘的文件中
	 * @param syncedTxid
	 * @param flushedTxid
	 * @return
	 */
	private Boolean existInFlushedFile(Long syncedTxid,String flushedTxid){
		String[] flushedTxidSplited = flushedTxid.split("_");

		Long startTxid = Long.valueOf(flushedTxidSplited[0]);
		Long endTxid = Long.valueOf(flushedTxidSplited[1]);
		Long fetchBeginTxid = syncedTxid + 1;
		if(fetchBeginTxid >= startTxid && fetchBeginTxid <= endTxid) {
			return true;
		}
		return false;
	}

	/**
	 * 从内存缓冲中拉取数据
	 * @param fetchedEditsLog
	 */
	private void fetchFromBufferedEditsLog(Long syncedTxid,JSONArray fetchedEditsLog){
		// 本次拉取的起始 txid
		Long fetchTxid = syncedTxid + 1;

		if(fetchTxid <= currentBufferedMaxTxid) {
			ThreadUtils.println("尝试从内存缓冲拉取的时候，发现上一次内存缓存有数据可供拉取......");
			fetchFromCurrentBuffer(syncedTxid, fetchedEditsLog);
			return;
		}
		// 清除之前拉取过的暂存数据
		currentBufferedEditsLog.clear();

		String[] bufferedEditsLog = namesystem.getFSEditlog().getBufferedEditsLog();

		// 从内存中获取不到任何数据
		if(bufferedEditsLog != null) {
			for(String editsLog : bufferedEditsLog) {
				currentBufferedEditsLog.add(JSONObject.parseObject(editsLog));
				// 我们在这里记录一下，当前内存缓存中的数据最大的一个txid是多少，这样下次再拉取可以
				// 判断，内存缓存里的数据是否还可以读取，不要每次都重新从内存缓冲中去加载
				currentBufferedMaxTxid = JSONObject.parseObject(editsLog).getLongValue("txid");
			}
			bufferedFlushedTxid = null;

			fetchFromCurrentBuffer(syncedTxid, fetchedEditsLog);
		}
	}

	/**
	 * 从当前已经在内存里缓存的数据中拉取editslog
	 * @param syncedTxid
	 * @param fetchedEditsLog
	 */
	private void fetchFromCurrentBuffer(Long syncedTxid, JSONArray fetchedEditsLog) {
		int fetchCount = 0;
		long fetchTxid = syncedTxid + 1;
		for(int i = 0; i < currentBufferedEditsLog.size(); i++) {
			if(currentBufferedEditsLog.getJSONObject(i).getLong("txid") == fetchTxid) {
				fetchedEditsLog.add(currentBufferedEditsLog.getJSONObject(i));
				fetchTxid = currentBufferedEditsLog.getJSONObject(i).getLong("txid") + 1;
				fetchCount++;
			}
			if(fetchCount >= BACKUP_NODE_FETCH_SIZE) {
				break;
			}
		}
	}

	/**
	 * 获取下一个磁盘文件对应的txid范围
	 * @param flushedTxids
	 * @param bufferedFlushedTxid
	 * @return
	 */
	private String getNextFlushedTxid(List<String> flushedTxids, String bufferedFlushedTxid){
		for(int i = 0; i < flushedTxids.size(); i++) {
			if(flushedTxids.get(i).equals(bufferedFlushedTxid)) {
				if(i + 1 < flushedTxids.size()) {
					return flushedTxids.get(i + 1);
				}
			}
		}
		return null;
	}
}
