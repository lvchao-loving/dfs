package com.lvchao.dfs.client;

import com.lvchao.dfs.namenode.rpc.model.*;
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

    @Override
    public Boolean upload(byte[] file, String filename, Long fileSize) throws Exception {
        if (!createFile(filename)){
            return false;
        }
        // 获取双副本系节点信息
        String dataNodes = allocateDataNodes(filename, fileSize);

        NIOClient.sendFile(file,fileSize);

        return true;
    }

    /**
     * 发送创建元数据请求
     * @param filename
     * @return
     */
    private Boolean createFile(String filename) {
        CreateFileRequest createFileRequest = CreateFileRequest.newBuilder().setFilename(filename).build();
        CreateFileResponse createFileResponse = namenode.create(createFileRequest);
        if (createFileResponse.getStatus() == 1){
            return true;
        }
        return false;
    }

    /**
     * 分配双副本对应的数据节点
     * @param filename
     * @param filesize
     * @return
     */
    private String allocateDataNodes(String filename, long filesize){
        AllocateDataNodesRequest request = AllocateDataNodesRequest.newBuilder().setFilename(filename).setFileSize(filesize).build();
        AllocateDataNodesResponse response = namenode.allocateDataNodes(request);
        return response.getDatanodes();
    }
}
