package com.lvchao.dfs.datanode.server;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.lvchao.dfs.namenode.rpc.model.HeartbeatResponse;

import java.io.File;
import java.util.Objects;


/**
 * @Title: HeartbeatManager
 * @Package: com.lvchao.dfs.datanode.server
 * @Description: 心跳管理组件
 * @auther: chao.lv
 * @date: 2021/11/24 15:11
 * @version: V1.0
 */
public class HeartbeatManager {
    public static final Integer SUCCESS = 1;
    public static final Integer FAILURE = 2;
    public static final Integer COMMAND_REGISTER = 1;
    public static final Integer COMMAND_REPORT_COMPLETE_STORAGE_INFO = 2;
    public static final Integer COMMAND_REPLICATE = 3;
    public static final Integer COMMAND_REMOVE_REPLICA = 4;

    private DataNodeConfig dataNodeConfig = new DataNodeConfig();

    private NameNodeRpcClient nameNodeRpcClient;

    private StorageManager storageManager;

    private ReplicateManager replicateManager;

    public HeartbeatManager(NameNodeRpcClient nameNodeRpcClient, StorageManager storageManager, ReplicateManager replicateManager) {
        this.nameNodeRpcClient = nameNodeRpcClient;
        this.storageManager = storageManager;
        this.replicateManager = replicateManager;
    }

    /**
     * 创建线程发送心跳，并启动线程
     */
    public void start() {
        HeartbeatThread heartbeatThread = new HeartbeatThread();
        heartbeatThread.setName("HeartbeatThread");
        heartbeatThread.start();
    }

    /**
     * 负责心跳的线程
     */
    class HeartbeatThread extends Thread {

        @Override
        public void run() {

            ThreadUtils.println("定时心跳线程启动.......");
            while (true) {
                try {
                    // 调用datanode组件发送注册心跳请求
                    HeartbeatResponse response = nameNodeRpcClient.heartbeat();

                    if (SUCCESS.equals(response.getStatus())){
                        JSONArray commandArray = JSONArray.parseArray(response.getCommands());
                        if (Objects.nonNull(commandArray) && commandArray.size() > 0){
                            for (int i = 0; i < commandArray.size(); i++) {
                                JSONObject command = commandArray.getJSONObject(i);
                                Integer type = command.getInteger("type");
                                JSONObject task = command.getJSONObject("content");

                                if (type.equals(COMMAND_REPLICATE)){
                                    replicateManager.addReplicateTask(task);
                                    ThreadUtils.println("接受副本复制任务" + command);
                                } else if (type.equals(COMMAND_REMOVE_REPLICA)){
                                    // 删除副本
                                    String filename = task.getString("filename");
                                    FileUtils fileUtils = new FileUtils();
                                    String absoluteFilename = fileUtils.getAbsoluteFilename(filename);
                                    File file = new File(absoluteFilename);
                                    if (file.exists()){
                                        file.delete();
                                    }
                                }
                            }
                        }
                    }else if (FAILURE.equals(response.getStatus())){
                        JSONArray commandArray = JSONArray.parseArray(response.getCommands());
                        for (int i = 0; i < commandArray.size(); i++) {
                            JSONObject command = commandArray.getJSONObject(i);
                            Integer type = command.getInteger("type");
                            // 处理注册的命令
                            if (type.equals(COMMAND_REGISTER)) {
                                nameNodeRpcClient.register();
                            }
                            // 处理全量上报的命令
                            else if (type.equals(COMMAND_REPORT_COMPLETE_STORAGE_INFO)){
                                StorageInfo dataNodeStoredInfo = storageManager.getDataNodeStoredInfo();
                                nameNodeRpcClient.reportCompleteStorageInfo(dataNodeStoredInfo);
                            }
                        }
                    }
                } catch(Exception e){
                    ThreadUtils.println("当前NameNode不可以用，心跳失败...");
                    e.printStackTrace();
                }
                try {
                    // 每隔30秒发送一次心跳到NameNode上去
                    Thread.sleep(30 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
