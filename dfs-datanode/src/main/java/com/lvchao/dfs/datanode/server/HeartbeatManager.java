package com.lvchao.dfs.datanode.server;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.lvchao.dfs.namenode.rpc.model.HeartbeatResponse;


/**
 * @Title: HeartbeatManager
 * @Package: com.lvchao.dfs.datanode.server
 * @Description: 心跳管理组件
 * @auther: chao.lv
 * @date: 2021/11/24 15:11
 * @version: V1.0
 */
public class HeartbeatManager {

    private DataNodeConfig dataNodeConfig = new DataNodeConfig();

    private NameNodeRpcClient nameNodeRpcClient;

    private StorageManager storageManager;

    public HeartbeatManager(NameNodeRpcClient nameNodeRpcClient, StorageManager storageManager) {
        this.nameNodeRpcClient = nameNodeRpcClient;
        this.storageManager = storageManager;
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

                    // 解析发送过来的心跳命令
                    if (response.getStatus() == 2) {
                        JSONArray commandArray = JSONArray.parseArray(response.getCommands());
                        for (int i = 0; i < commandArray.size(); i++) {
                            JSONObject command = commandArray.getJSONObject(i);
                            Integer type = command.getInteger("type");
                            if (type.equals(1)) {
                                nameNodeRpcClient.register();
                            } else {
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
