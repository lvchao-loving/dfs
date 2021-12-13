# namenode节点

# DataNodeManager组件
作用：管理集群中注册到NameNode节点中DataNode节点数据


DataNode逻辑
1.【datanode】启动时向【namenode】注册【datanode】相关的信息；
    1.1.【datanode】节点通过返回值进行处理：
        true：查询当前节点下的所存储的数据大小和各个文件的名称上报给【namenode】
        false：不进行任何处理
    1.2.发送心跳请求
    1.3.启动图片上传主线程

2.【datanode】在接收图片数据存储到本地后，向【NameNode】发送增量日志

3.【datanode】的【HeartbeatThread】线程向【namenode】发送心跳请求，接收【datanode】返回的结果（namenode处理的逻辑为第3条）；
    SUCCESS：遍历所有指令List<Command>
        COMMAND_REPLICATE：处理副本复制任务
            将命令添加到阻塞队列中【ConcurrentLinkedQueue<JSONObject> replicateTaskQueue】，第4条
        COMMAND_REMOVE_REPLICA：处理副本移除任务
            获取命令中的文件路径，删除当前节点下的文件
    FAILURE：遍历所有指令List<Command>
        COMMAND_REGISTER：处理发送注册请求
        COMMAND_REPORT_COMPLETE_STORAGE_INFO：处理发送全量存储信息
        
4.【datanode】的【ReplicateWorker】线程消费【replicateTaskQueue】队列中的数据：
    1）处理队列中的数据，将队列中的数据取出并根据原数据节点的信息读取文件持久化到当前【datanode】节点中
    2）上报当前节点的存储日志

5.【datanode】在接收完文件并成功存储到本地后将发送增量日志到【namenode】节点


NameNode逻辑
1.【namenode】接收【datanode】注册请求时，如果【namenode】节点中存在【datanode】节点相关信息在则说明注册过返回false，否则返回true；

2.【namenode】接收【datanode】全量上报信息（增量上报信息时直接操作）时，遍历每一条日志信息（图片存储的ip|hostname|filename|filelength）
    2.1.增加 Map<String,List<DataNodeInfo>> replicasByFilename 存储信息
    2.2.判断是否超过了副本的限制，如果超过则向DataNodeInfo中添加RemoveReplicaTask任务
    2.3.增加 Map<String,List<String>> filesByDatanode 存储信息

3.【namenode】接收【datanode】心跳请求
    3.1.将注册的心跳信息从 Map<String, DataNodeInfo> datanodeInfoMap 获取，存在返回true，不存在返回false；
    SUCCESS（说明之前注册过）:
        1)将处理【DataNodeInfo】中ReplicateTask队列的数据
            将ReplicateTask封装成指令List<Command>
        2)将处理【DataNodeInfo】中RemoveReplicaTask队列的数据
            将RemoveReplicaTask封装成指令List<Command>
    FAILURE（说明之前没有注册过，后者已断开）:
        1)封装Command请求，封装全量上传命令List<Command>

4.【namenode】处理【client】发送过来的分配【datanode】节点的请求
    将注册在【namenode】的【datanode】数据取出来，然后对其升序排序，取出前两条数据，对分配的数据进行增加存储数据，并返回【datanode】数据