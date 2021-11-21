package com.lvchao.dfs.namenode.server;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @Title: DataNodeManager
 * @Package: com.lvchao.dfs.namenode.server
 * @Description: 负责管理集群中所有的 DataNode节点
 * @auther: chao.lv
 * @date: 2021/11/19 6:56
 * @version: V1.0
 */
public class DataNodeManager {
    /**
     * 集群中所有的datanode
     */
    private Map<String, DataNodeInfo> datanodeInfoMap = new ConcurrentHashMap<String, DataNodeInfo>();

    public DataNodeManager() {
        // 启动内部监控线程
        DataNodeAliveMonitor dataNodeAliveMonitor = new DataNodeAliveMonitor();
        dataNodeAliveMonitor.setName("DataNodeAliveMonitor");
        dataNodeAliveMonitor.start();
    }

    /**
     * datanode进行注册
     * @param ip
     * @param hostname
     */
    public Boolean register(String ip, String hostname,Integer nioPort) {
        DataNodeInfo datanode = new DataNodeInfo(ip, hostname,nioPort);
        datanodeInfoMap.put(ip + "-" + hostname, datanode);
        ThreadUntils.println("DataNode注册：ip=" + ip + ",hostname=" + hostname + ",nioPort=" + nioPort);
        return true;
    }

    /**
     * datanode进行心跳
     * @param ip
     * @param hostname
     * @return
     */
    public Boolean heartbeat(String ip, String hostname) {
        DataNodeInfo datanode = datanodeInfoMap.get(ip + "-" + hostname);
        datanode.setLatestHeartbeatTime(System.currentTimeMillis());
        ThreadUntils.println("DataNode发送心跳：ip=" + ip + ",hostname=" + hostname);
        return true;
    }

    /**
     * 根据上传的文件大小，分配双副本对应的数据节点
     * @param fileSize
     * @return
     */
    public List<DataNodeInfo> allocateDataNodes(Long fileSize){
        synchronized (this){

            List<DataNodeInfo> dataNodeInfoList = new ArrayList<>();
            for (String dniKey:datanodeInfoMap.keySet()){
                dataNodeInfoList.add(datanodeInfoMap.get(dniKey));
            }

            if (dataNodeInfoList.size() < 2){
                throw new RuntimeException("DataNodeInfo 注册的节点过少");
            }

            if (dataNodeInfoList.size() == 2){
                return dataNodeInfoList;
            }

            return dataNodeInfoList.stream().sorted(Comparator.comparing(DataNodeInfo::getStoredDataSize)).collect(Collectors.toList());
        }
    }

    /**
     * 根据 key 获取 DataNodeInfo
     * @param ip
     * @param hostname
     * @return
     */
    public DataNodeInfo getDataNodeInfo(String ip, String hostname){
        return datanodeInfoMap.get(ip + "-" + hostname);
    }

    /**
     * datanode是否存活的监控线程
     */
    class DataNodeAliveMonitor extends Thread {

        @Override
        public void run() {
            try {
                while(true) {
                    List<String> toRemoveDatanodes = new ArrayList<String>();

                    Iterator<DataNodeInfo> datanodesIterator = datanodeInfoMap.values().iterator();
                    DataNodeInfo datanode = null;
                    while(datanodesIterator.hasNext()) {
                        datanode = datanodesIterator.next();
                        if(System.currentTimeMillis() - datanode.getLatestHeartbeatTime() > 9000 * 1000) {
                            toRemoveDatanodes.add(datanode.getIp() + "-" + datanode.getHostname());
                        }
                    }

                    if(!toRemoveDatanodes.isEmpty()) {
                        synchronized (this){
                            for(String toRemoveDatanode : toRemoveDatanodes) {
                                datanodeInfoMap.remove(toRemoveDatanode);
                            }
                        }
                    }

                    Thread.sleep(3000 * 1000);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
}
