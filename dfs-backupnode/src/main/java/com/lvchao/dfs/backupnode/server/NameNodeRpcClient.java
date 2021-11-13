package com.lvchao.dfs.backupnode.server;

import com.alibaba.fastjson.JSONArray;
import com.lvchao.dfs.namenode.rpc.model.FetchEditsLogRequest;
import com.lvchao.dfs.namenode.rpc.model.FetchEditsLogResponse;
import com.lvchao.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest;
import com.lvchao.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse;
import com.lvchao.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;

/**
 * @Title: NameNodeRpcClient
 * @Package: com.lvchao.dfs.backupnode.server
 * @Description:
 * @auther: chao.lv
 * @date: 2021/10/26 15:06
 * @version: V1.0
 */
public class NameNodeRpcClient {
    private static final String NAMENODE_HOSTNAME = "localhost";
    private static final Integer NAMENODE_PORT = 50070;

    private NameNodeServiceGrpc.NameNodeServiceBlockingStub namenode;

    /**
     * 初始化 rpc
     */
    public NameNodeRpcClient() {
        ManagedChannel channel = NettyChannelBuilder
                .forAddress(NAMENODE_HOSTNAME, NAMENODE_PORT)
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();
        this.namenode = NameNodeServiceGrpc.newBlockingStub(channel);
        ThreadUntils.println("初始化 NameNodeRpcClient 完成");
    }

    /**
     * 拉取 editslog 数据
     * @return
     */
    public JSONArray fetchEditsLog(Long syncedTxid) {
        Integer fetchEditsLogCode = 1;
        FetchEditsLogRequest request = FetchEditsLogRequest.newBuilder().setSyncedTxid(syncedTxid).build();

        FetchEditsLogResponse response = namenode.fetchEditsLog(request);
        String editsLogJson = response.getEditsLog();

        return JSONArray.parseArray(editsLogJson);
    }

    /**
     * 更新 checkpoint 的 txid
     * @param txid
     */
    public void updateCheckpointTxid(Long txid){
        // 第一次启动，暂无需要同步的 txid，不需要发送数据
        if (txid == 0){
            ThreadUntils.println("第一次启动，暂无需要同步的 txid，不需要发送数据");
            return;
        }
        ThreadUntils.println("同步当前的 txid = " + txid);
        UpdateCheckpointTxidRequest updateCheckpointTxidRequest = UpdateCheckpointTxidRequest.newBuilder().setTxid(txid).build();
        UpdateCheckpointTxidResponse updateCheckpointTxidResponse = namenode.updateCheckpointTxid(updateCheckpointTxidRequest);
        updateCheckpointTxidResponse.getStatus();
    }
}
