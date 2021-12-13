package com.lvchao.dfs.namenode.server;

import com.lvchao.dfs.datanode.server.DataNodeConfig;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
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
    /**
     * 负责管理元数据的核心组件
     */
    private FSNamesystem fsNamesystem;

    public DataNodeManager() {
        // 启动内部监控线程
        DataNodeAliveMonitor dataNodeAliveMonitor = new DataNodeAliveMonitor();
        dataNodeAliveMonitor.setName("DataNodeAliveMonitor");
        dataNodeAliveMonitor.start();
    }

    public void setNamesystem(FSNamesystem fsNamesystem){
        this.fsNamesystem = fsNamesystem;
    }

    /**
     * datanode进行注册
     * @param ip
     * @param hostname
     */
    public Boolean register(String ip, String hostname,Integer nioPort) {
        String mapKey = ip + "_" + hostname;
        if (datanodeInfoMap.containsKey(mapKey)){
            ThreadUtils.println("当前主机已注册到NameNode节点：ip+hostname=" + mapKey);
            return false;
        }

        DataNodeInfo datanode = new DataNodeInfo(ip, hostname,nioPort);
        datanodeInfoMap.put(mapKey, datanode);
        ThreadUtils.println("DataNode注册：ip=" + ip + ",hostname=" + hostname + ",nioPort=" + nioPort);
        return true;
    }

    /**
     * datanode进行心跳
     * @param ip
     * @param hostname
     * @return
     */
    public Boolean heartbeat(String ip, String hostname) {
        DataNodeInfo datanode = datanodeInfoMap.get(ip + "_" + hostname);
        datanode.setLatestHeartbeatTime(System.currentTimeMillis());
        ThreadUtils.println("DataNode发送心跳：ip=" + ip + ",hostname=" + hostname);
        return true;
    }

    /**
     * 根据上传的文件大小，分配双副本对应的数据节点
     * @param fileSize
     * @return
     */
    public List<DataNodeInfo> allocateDataNodes(Long fileSize) {
        synchronized (this) {
            List<DataNodeInfo> dataNodeInfoList = new ArrayList<>(datanodeInfoMap.values());

            if (dataNodeInfoList.size() < 2) {
                throw new RuntimeException("DataNodeInfo 注册的节点过少");
            }

            // 取出前两条数据
            List<DataNodeInfo> dataNodeInfoListSelected = dataNodeInfoList.stream()
                    .sorted(Comparator.comparing(DataNodeInfo::getStoredDataSize))
                    .collect(Collectors.toList()).subList(0, 2);

            // 对分配的数据进行增加存储数据
            dataNodeInfoListSelected.forEach(item -> item.addStoredDataSize(fileSize));

            return dataNodeInfoListSelected;
        }
    }

    /**
     * 根据 key 获取 DataNodeInfo
     * @param ip
     * @param hostname
     * @return
     */
    public DataNodeInfo getDataNodeInfo(String ip, String hostname){
        return datanodeInfoMap.get(ip + "_" + hostname);
    }

    /**
     * 设置一个 DataNodeInfo 的存储数据的大小
     * @param ip
     * @param hostname
     * @param storedDataSize
     */
    public void setStoredDataSize(String ip, String hostname, Long storedDataSize){
        // 从 DataNodeInfoMap 中获取 DataNodeInfo
        DataNodeInfo dataNodeInfo = datanodeInfoMap.get(ip + "_" + hostname);
        // 设置 DataNodeInfo 的 stordDataSize
        dataNodeInfo.setStoredDataSize(storedDataSize);
    }

    /**
     * 创建丢失副本的复制任务
     * @param dataNodeInfo
     */
    public void createLostReplicaTask(DataNodeInfo dataNodeInfo){
        // 根据 hostname 获取存储的所有文件
        List<String> filesByDatanode = fsNamesystem.getFilesByDatanode(dataNodeInfo.getIp(), dataNodeInfo.getHostname());
        if (filesByDatanode == null || filesByDatanode.size() == 0){
            return;
        }
        for (String file:filesByDatanode) {
            String filename = file.split("_")[0];
            Long fileLength = Long.valueOf(file.split("_")[1]);
            // 存活的原 DataNodeInfo 节点
            DataNodeInfo sourceDataNodeInfo = fsNamesystem.getReplicateSource(filename, dataNodeInfo);
            // 目标 DataNodeInfo 节点
            DataNodeInfo destNodeInfo = allocateReplicateDataNode(fileLength, sourceDataNodeInfo, dataNodeInfo);


            ReplicateTask replicateTask = new ReplicateTask(filename, fileLength, sourceDataNodeInfo, destNodeInfo);
            // 将复制任务放到目标数据节点的任务队列里
            destNodeInfo.addReplicateTask(replicateTask);

            ThreadUtils.println("为目标数据节点生成一个副本复制任务：" + replicateTask);
        }
    }

    /**
     * 分配用来复制副本的数据节点
     * @param filesize
     * @param sourceDataNodeInfo
     * @param deadDataNodeInfo
     * @return
     */
    public DataNodeInfo allocateReplicateDataNode(Long filesize, DataNodeInfo sourceDataNodeInfo, DataNodeInfo deadDataNodeInfo){

        synchronized (this){
            List<DataNodeInfo> dataNodeInfoList = new ArrayList<>(datanodeInfoMap.values());

            // 去除挂掉节点的备份节点和挂掉的节点
            dataNodeInfoList = dataNodeInfoList.stream().filter(item -> (!item.equals(sourceDataNodeInfo)) && (!item.equals(deadDataNodeInfo))).collect(Collectors.toList());

            if (dataNodeInfoList.size() == 0){
                throw new RuntimeException("datanode数据节点不存");
            }

            // 对所有存活的 DataNodeInfo 节点排序
            dataNodeInfoList = dataNodeInfoList.stream().sorted(Comparator.comparing(DataNodeInfo::getStoredDataSize)).collect(Collectors.toList());
            DataNodeInfo dataNodeInfo = null;
            if (dataNodeInfoList.size() > 0){
                dataNodeInfo = dataNodeInfoList.get(0);
                dataNodeInfo.addStoredDataSize(filesize);
            }
            return dataNodeInfo;
        }
    }

    /**
     * datanode是否存活的监控线程
     */
    class DataNodeAliveMonitor extends Thread {

        @Override
        public void run() {
            try {
                while(true) {
                    List<DataNodeInfo> toRemoveDataNodeInfoList = new ArrayList<DataNodeInfo>();

                    Iterator<DataNodeInfo> datanodesIterator = datanodeInfoMap.values().iterator();

                    DataNodeInfo dataNodeInfo = null;
                    while(datanodesIterator.hasNext()) {
                        dataNodeInfo = datanodesIterator.next();
                        // 每 60s 执行一次检测
                        if(System.currentTimeMillis() - dataNodeInfo.getLatestHeartbeatTime() > 60 * 1000) {
                            toRemoveDataNodeInfoList.add(dataNodeInfo);
                        }
                    }
                    if(!toRemoveDataNodeInfoList.isEmpty()) {
                        synchronized (this){
                            for(DataNodeInfo toRemoveDatanode : toRemoveDataNodeInfoList) {
                                ThreadUtils.println("数据节点【" + toRemoveDatanode + "】宕机，需要进行副本复制...");
                                // 创建丢失 DataNodeInfo 节点任务
                                createLostReplicaTask(toRemoveDatanode);

                                datanodeInfoMap.remove(toRemoveDatanode.getIp() + "_" + toRemoveDatanode.getHostname());
                                ThreadUtils.println("从内存数据结构中删除掉这个数据节点：" + datanodeInfoMap);
                                // 删除掉失去心跳的DataNodeInfo节点相关的缓存信息
                                fsNamesystem.removeDeadDatanode(toRemoveDatanode);
                            }
                        }
                    }
                    Thread.sleep(30 * 1000);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
