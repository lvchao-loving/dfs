package com.lvchao.dfs.client;

import com.lvchao.dfs.namenode.rpc.model.MkdirRequest;
import com.lvchao.dfs.namenode.rpc.model.MkdirResponse;
import com.lvchao.dfs.namenode.rpc.model.ShutdownRequest;
import com.lvchao.dfs.namenode.rpc.model.ShutdownResponse;
import com.lvchao.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;

/**
 * @Title: FileSystemImpl
 * @Package: com.lvchao.dfs.client
 * @Description: 文件系统客户端的实现类
 * @auther: chao.lv
 * @date: 2021/10/22 21:08
 * @version: V1.0
 */
public class FileSystemImpl implements FileSystem{
    private static final String NAMENODE_HOSTNAME = "localhost";
    private static final Integer NAMENODE_PORT = 50070;

    private NameNodeServiceGrpc.NameNodeServiceBlockingStub namenode;

    public FileSystemImpl(){
        ManagedChannel channel = NettyChannelBuilder
                .forAddress(NAMENODE_HOSTNAME, NAMENODE_PORT)
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();
        this.namenode = NameNodeServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public void mkdir(String path) throws Exception {
        MkdirRequest mkdirRequest = MkdirRequest.newBuilder().setPath(path).build();
        MkdirResponse mkdirResponse = namenode.mkdir(mkdirRequest);

        ThreadUntils.println("创建目录的响应：" + mkdirResponse.getStatus());
    }

    @Override
    public void shutdown() throws Exception {
        ShutdownRequest shutdownRequest = ShutdownRequest.newBuilder().setCode(1).build();
        ShutdownResponse shutdownResponse = namenode.shutdown(shutdownRequest);

        ThreadUntils.println("关闭请求的响应：" + shutdownResponse.getStatus());
    }
}
