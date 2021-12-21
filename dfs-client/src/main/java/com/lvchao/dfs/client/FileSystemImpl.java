package com.lvchao.dfs.client;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.lvchao.dfs.namenode.rpc.model.*;
import com.lvchao.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import org.apache.commons.lang3.StringUtils;

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
    private NIOClient nioClient;

    public FileSystemImpl(){
        ManagedChannel channel = NettyChannelBuilder
                .forAddress(NAMENODE_HOSTNAME, NAMENODE_PORT)
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();
        this.namenode = NameNodeServiceGrpc.newBlockingStub(channel);
        this.nioClient = new NIOClient();
    }

    @Override
    public void mkdir(String path) throws Exception {
        MkdirRequest mkdirRequest = MkdirRequest.newBuilder().setPath(path).build();
        MkdirResponse mkdirResponse = namenode.mkdir(mkdirRequest);

        ThreadUtils.println("创建目录的响应：" + mkdirResponse.getStatus());
    }

    @Override
    public void shutdown() throws Exception {
        ShutdownRequest shutdownRequest = ShutdownRequest.newBuilder().setCode(1).build();
        ShutdownResponse shutdownResponse = namenode.shutdown(shutdownRequest);

        ThreadUtils.println("关闭请求的响应：" + shutdownResponse.getStatus());
    }

    @Override
    public Boolean upload(byte[] file, String filename, Long fileSize) throws Exception {
        if (!createFile(filename)){
            return false;
        }
        // 获取双副本系节点信息
        String dataNodeJson = allocateDataNodes(filename, fileSize);

        ThreadUtils.println("NameNode分配的datanode节点为：" + dataNodeJson);

        JSONArray datanodeArray = JSONArray.parseArray(dataNodeJson);

        for (int i = 0; i < datanodeArray.size(); i++) {
            JSONObject datanode = datanodeArray.getJSONObject(i);
            String hostname = datanode.getString("hostname");
            String ip = datanode.getString("ip");
            Integer nioPort = datanode.getIntValue("nioPort");
            Boolean result = nioClient.sendFile(hostname, nioPort, file, filename, fileSize);
            // 通过nio发送图片没有成功
            if (!result){
                // 重新向namenode节点发送获取有效的datanode
                datanode = JSONObject.parseObject(reallocateDataNode(filename, fileSize,  ip + "_" + hostname));
                hostname = datanode.getString("hostname");
                nioPort = datanode.getIntValue("nioPort");
                // 再次发送
                result = nioClient.sendFile(hostname, nioPort, file, filename, fileSize);
                if (!result){
                    throw new RuntimeException("file upload failed......");
                }
            }
        }

        return true;
    }

    /**
     * 重新分配一个数据节点
     * @param filename
     * @param fileSize
     * @param excludedDataNodeId
     * @return
     */
    private String reallocateDataNode(String filename, Long fileSize, String excludedDataNodeId){
        ReallocateDataNodeRequest reallocateDataNodeRequest = ReallocateDataNodeRequest.newBuilder().setFileSize(fileSize).setExcludedDataNodeId(excludedDataNodeId).build();
        ReallocateDataNodeResponse reallocateDataNodeResponse = namenode.reallocateDataNode(reallocateDataNodeRequest);
        return reallocateDataNodeResponse.getDatanode();
    }

    /**
     * 下载文件
     * @param filename 文件名称
     * @return 文件字节数组
     * @throws Exception
     */
    @Override
    public byte[] download(String filename) throws Exception {
        JSONObject dataNodeInfoJSON =  chooseDataNodeFromReplicas(filename,"");

        String hostname = dataNodeInfoJSON.getString("hostname");
        String ip = dataNodeInfoJSON.getString("ip");
        Integer nioPort = dataNodeInfoJSON.getInteger("nioPort");
        byte[] file = null;
        try {
            file = nioClient.readFile(hostname, nioPort, filename);
        } catch (Exception e) {
            // 下载失败使用其他datanode节点重新下载
            dataNodeInfoJSON = chooseDataNodeFromReplicas(filename, ip + "_" + hostname);
            hostname = dataNodeInfoJSON.getString("hostname");
            nioPort = dataNodeInfoJSON.getInteger("nioPort");

            try{
                file = nioClient.readFile(hostname, nioPort, filename);
            }catch (Exception exception){
                throw new RuntimeException("服务运行异常");
            }
        }
        return null;
    }

    /**
     * 根据文件名称获取存储文件 DataNode 节点，并排除指定的 datanode 节点
     * @param filename
     * @param excludedDataNodeId
     * @return
     */
    private JSONObject chooseDataNodeFromReplicas(String filename, String excludedDataNodeId){
        ChooseDataNodeFromReplicasRequest request = ChooseDataNodeFromReplicasRequest.newBuilder().setFilename(filename).setExcludedDataNodeId(excludedDataNodeId).build();
        ChooseDataNodeFromReplicasResponse chooseDataNodeFromReplicasResponse = namenode.chooseDataNodeFromReplicas(request);
        String datanode = chooseDataNodeFromReplicasResponse.getDatanode();
        if (StringUtils.isBlank(datanode)){
            throw new RuntimeException("执行chooseDataNodeFromReplicas方法获取datanode节点异常");
        }
        return JSONObject.parseObject(datanode);
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
