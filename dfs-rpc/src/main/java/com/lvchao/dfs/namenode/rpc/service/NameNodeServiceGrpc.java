package com.lvchao.dfs.namenode.rpc.service;

import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;

@javax.annotation.Generated("by gRPC proto compiler")
public class NameNodeServiceGrpc {

  private NameNodeServiceGrpc() {}

  public static final String SERVICE_NAME = "com.lvchao.dfs.namenode.rpc.NameNodeService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.lvchao.dfs.namenode.rpc.model.RegisterRequest,
      com.lvchao.dfs.namenode.rpc.model.RegisterResponse> METHOD_REGISTER =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "com.lvchao.dfs.namenode.rpc.NameNodeService", "register"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.lvchao.dfs.namenode.rpc.model.RegisterRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.lvchao.dfs.namenode.rpc.model.RegisterResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.lvchao.dfs.namenode.rpc.model.HeartbeatRequest,
      com.lvchao.dfs.namenode.rpc.model.HeartbeatResponse> METHOD_HEARTBEAT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "com.lvchao.dfs.namenode.rpc.NameNodeService", "heartbeat"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.lvchao.dfs.namenode.rpc.model.HeartbeatRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.lvchao.dfs.namenode.rpc.model.HeartbeatResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.lvchao.dfs.namenode.rpc.model.MkdirRequest,
      com.lvchao.dfs.namenode.rpc.model.MkdirResponse> METHOD_MKDIR =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "com.lvchao.dfs.namenode.rpc.NameNodeService", "Mkdir"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.lvchao.dfs.namenode.rpc.model.MkdirRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.lvchao.dfs.namenode.rpc.model.MkdirResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.lvchao.dfs.namenode.rpc.model.ShutdownRequest,
      com.lvchao.dfs.namenode.rpc.model.ShutdownResponse> METHOD_SHUTDOWN =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "com.lvchao.dfs.namenode.rpc.NameNodeService", "shutdown"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.lvchao.dfs.namenode.rpc.model.ShutdownRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.lvchao.dfs.namenode.rpc.model.ShutdownResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.lvchao.dfs.namenode.rpc.model.FetchEditsLogRequest,
      com.lvchao.dfs.namenode.rpc.model.FetchEditsLogResponse> METHOD_FETCH_EDITS_LOG =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "com.lvchao.dfs.namenode.rpc.NameNodeService", "fetchEditsLog"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.lvchao.dfs.namenode.rpc.model.FetchEditsLogRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.lvchao.dfs.namenode.rpc.model.FetchEditsLogResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.lvchao.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest,
      com.lvchao.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse> METHOD_UPDATE_CHECKPOINT_TXID =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "com.lvchao.dfs.namenode.rpc.NameNodeService", "updateCheckpointTxid"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.lvchao.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.lvchao.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.lvchao.dfs.namenode.rpc.model.CreateFileRequest,
      com.lvchao.dfs.namenode.rpc.model.CreateFileResponse> METHOD_CREATE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "com.lvchao.dfs.namenode.rpc.NameNodeService", "create"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.lvchao.dfs.namenode.rpc.model.CreateFileRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.lvchao.dfs.namenode.rpc.model.CreateFileResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.lvchao.dfs.namenode.rpc.model.AllocateDataNodesRequest,
      com.lvchao.dfs.namenode.rpc.model.AllocateDataNodesResponse> METHOD_ALLOCATE_DATA_NODES =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "com.lvchao.dfs.namenode.rpc.NameNodeService", "allocateDataNodes"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.lvchao.dfs.namenode.rpc.model.AllocateDataNodesRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.lvchao.dfs.namenode.rpc.model.AllocateDataNodesResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.lvchao.dfs.namenode.rpc.model.InformReplicaReceivedRequest,
      com.lvchao.dfs.namenode.rpc.model.InformReplicaReceivedResponse> METHOD_INFORM_REPLICA_RECEIVED =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "com.lvchao.dfs.namenode.rpc.NameNodeService", "InformReplicaReceived"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.lvchao.dfs.namenode.rpc.model.InformReplicaReceivedRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.lvchao.dfs.namenode.rpc.model.InformReplicaReceivedResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.lvchao.dfs.namenode.rpc.model.ReportCompleteStorageInfoRequest,
      com.lvchao.dfs.namenode.rpc.model.ReportCompleteStorageInfoResponse> METHOD_REPORT_COMPLETE_STORAGE_INFO =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "com.lvchao.dfs.namenode.rpc.NameNodeService", "reportCompleteStorageInfo"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.lvchao.dfs.namenode.rpc.model.ReportCompleteStorageInfoRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.lvchao.dfs.namenode.rpc.model.ReportCompleteStorageInfoResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi
  public static final io.grpc.MethodDescriptor<com.lvchao.dfs.namenode.rpc.model.GetDataNodeForFileRequest,
      com.lvchao.dfs.namenode.rpc.model.GetDataNodeForFileResponse> METHOD_GET_DATA_NODE_FOR_FILE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "com.lvchao.dfs.namenode.rpc.NameNodeService", "getDataNodeForFile"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.lvchao.dfs.namenode.rpc.model.GetDataNodeForFileRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.lvchao.dfs.namenode.rpc.model.GetDataNodeForFileResponse.getDefaultInstance()));

  public static NameNodeServiceStub newStub(io.grpc.Channel channel) {
    return new NameNodeServiceStub(channel);
  }

  public static NameNodeServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new NameNodeServiceBlockingStub(channel);
  }

  public static NameNodeServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new NameNodeServiceFutureStub(channel);
  }

  public static interface NameNodeService {

    public void register(com.lvchao.dfs.namenode.rpc.model.RegisterRequest request,
        io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.RegisterResponse> responseObserver);

    public void heartbeat(com.lvchao.dfs.namenode.rpc.model.HeartbeatRequest request,
        io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.HeartbeatResponse> responseObserver);

    public void mkdir(com.lvchao.dfs.namenode.rpc.model.MkdirRequest request,
        io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.MkdirResponse> responseObserver);

    public void shutdown(com.lvchao.dfs.namenode.rpc.model.ShutdownRequest request,
        io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.ShutdownResponse> responseObserver);

    public void fetchEditsLog(com.lvchao.dfs.namenode.rpc.model.FetchEditsLogRequest request,
        io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.FetchEditsLogResponse> responseObserver);

    public void updateCheckpointTxid(com.lvchao.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest request,
        io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse> responseObserver);

    public void create(com.lvchao.dfs.namenode.rpc.model.CreateFileRequest request,
        io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.CreateFileResponse> responseObserver);

    public void allocateDataNodes(com.lvchao.dfs.namenode.rpc.model.AllocateDataNodesRequest request,
        io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.AllocateDataNodesResponse> responseObserver);

    public void informReplicaReceived(com.lvchao.dfs.namenode.rpc.model.InformReplicaReceivedRequest request,
        io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.InformReplicaReceivedResponse> responseObserver);

    public void reportCompleteStorageInfo(com.lvchao.dfs.namenode.rpc.model.ReportCompleteStorageInfoRequest request,
        io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.ReportCompleteStorageInfoResponse> responseObserver);

    public void getDataNodeForFile(com.lvchao.dfs.namenode.rpc.model.GetDataNodeForFileRequest request,
        io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.GetDataNodeForFileResponse> responseObserver);
  }

  public static interface NameNodeServiceBlockingClient {

    public com.lvchao.dfs.namenode.rpc.model.RegisterResponse register(com.lvchao.dfs.namenode.rpc.model.RegisterRequest request);

    public com.lvchao.dfs.namenode.rpc.model.HeartbeatResponse heartbeat(com.lvchao.dfs.namenode.rpc.model.HeartbeatRequest request);

    public com.lvchao.dfs.namenode.rpc.model.MkdirResponse mkdir(com.lvchao.dfs.namenode.rpc.model.MkdirRequest request);

    public com.lvchao.dfs.namenode.rpc.model.ShutdownResponse shutdown(com.lvchao.dfs.namenode.rpc.model.ShutdownRequest request);

    public com.lvchao.dfs.namenode.rpc.model.FetchEditsLogResponse fetchEditsLog(com.lvchao.dfs.namenode.rpc.model.FetchEditsLogRequest request);

    public com.lvchao.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse updateCheckpointTxid(com.lvchao.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest request);

    public com.lvchao.dfs.namenode.rpc.model.CreateFileResponse create(com.lvchao.dfs.namenode.rpc.model.CreateFileRequest request);

    public com.lvchao.dfs.namenode.rpc.model.AllocateDataNodesResponse allocateDataNodes(com.lvchao.dfs.namenode.rpc.model.AllocateDataNodesRequest request);

    public com.lvchao.dfs.namenode.rpc.model.InformReplicaReceivedResponse informReplicaReceived(com.lvchao.dfs.namenode.rpc.model.InformReplicaReceivedRequest request);

    public com.lvchao.dfs.namenode.rpc.model.ReportCompleteStorageInfoResponse reportCompleteStorageInfo(com.lvchao.dfs.namenode.rpc.model.ReportCompleteStorageInfoRequest request);

    public com.lvchao.dfs.namenode.rpc.model.GetDataNodeForFileResponse getDataNodeForFile(com.lvchao.dfs.namenode.rpc.model.GetDataNodeForFileRequest request);
  }

  public static interface NameNodeServiceFutureClient {

    public com.google.common.util.concurrent.ListenableFuture<com.lvchao.dfs.namenode.rpc.model.RegisterResponse> register(
        com.lvchao.dfs.namenode.rpc.model.RegisterRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.lvchao.dfs.namenode.rpc.model.HeartbeatResponse> heartbeat(
        com.lvchao.dfs.namenode.rpc.model.HeartbeatRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.lvchao.dfs.namenode.rpc.model.MkdirResponse> mkdir(
        com.lvchao.dfs.namenode.rpc.model.MkdirRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.lvchao.dfs.namenode.rpc.model.ShutdownResponse> shutdown(
        com.lvchao.dfs.namenode.rpc.model.ShutdownRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.lvchao.dfs.namenode.rpc.model.FetchEditsLogResponse> fetchEditsLog(
        com.lvchao.dfs.namenode.rpc.model.FetchEditsLogRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.lvchao.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse> updateCheckpointTxid(
        com.lvchao.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.lvchao.dfs.namenode.rpc.model.CreateFileResponse> create(
        com.lvchao.dfs.namenode.rpc.model.CreateFileRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.lvchao.dfs.namenode.rpc.model.AllocateDataNodesResponse> allocateDataNodes(
        com.lvchao.dfs.namenode.rpc.model.AllocateDataNodesRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.lvchao.dfs.namenode.rpc.model.InformReplicaReceivedResponse> informReplicaReceived(
        com.lvchao.dfs.namenode.rpc.model.InformReplicaReceivedRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.lvchao.dfs.namenode.rpc.model.ReportCompleteStorageInfoResponse> reportCompleteStorageInfo(
        com.lvchao.dfs.namenode.rpc.model.ReportCompleteStorageInfoRequest request);

    public com.google.common.util.concurrent.ListenableFuture<com.lvchao.dfs.namenode.rpc.model.GetDataNodeForFileResponse> getDataNodeForFile(
        com.lvchao.dfs.namenode.rpc.model.GetDataNodeForFileRequest request);
  }

  public static class NameNodeServiceStub extends io.grpc.stub.AbstractStub<NameNodeServiceStub>
      implements NameNodeService {
    private NameNodeServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private NameNodeServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected NameNodeServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new NameNodeServiceStub(channel, callOptions);
    }

    @java.lang.Override
    public void register(com.lvchao.dfs.namenode.rpc.model.RegisterRequest request,
        io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.RegisterResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_REGISTER, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void heartbeat(com.lvchao.dfs.namenode.rpc.model.HeartbeatRequest request,
        io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.HeartbeatResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_HEARTBEAT, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void mkdir(com.lvchao.dfs.namenode.rpc.model.MkdirRequest request,
        io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.MkdirResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_MKDIR, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void shutdown(com.lvchao.dfs.namenode.rpc.model.ShutdownRequest request,
        io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.ShutdownResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_SHUTDOWN, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void fetchEditsLog(com.lvchao.dfs.namenode.rpc.model.FetchEditsLogRequest request,
        io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.FetchEditsLogResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_FETCH_EDITS_LOG, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void updateCheckpointTxid(com.lvchao.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest request,
        io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_UPDATE_CHECKPOINT_TXID, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void create(com.lvchao.dfs.namenode.rpc.model.CreateFileRequest request,
        io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.CreateFileResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CREATE, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void allocateDataNodes(com.lvchao.dfs.namenode.rpc.model.AllocateDataNodesRequest request,
        io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.AllocateDataNodesResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_ALLOCATE_DATA_NODES, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void informReplicaReceived(com.lvchao.dfs.namenode.rpc.model.InformReplicaReceivedRequest request,
        io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.InformReplicaReceivedResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_INFORM_REPLICA_RECEIVED, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void reportCompleteStorageInfo(com.lvchao.dfs.namenode.rpc.model.ReportCompleteStorageInfoRequest request,
        io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.ReportCompleteStorageInfoResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_REPORT_COMPLETE_STORAGE_INFO, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void getDataNodeForFile(com.lvchao.dfs.namenode.rpc.model.GetDataNodeForFileRequest request,
        io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.GetDataNodeForFileResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_DATA_NODE_FOR_FILE, getCallOptions()), request, responseObserver);
    }
  }

  public static class NameNodeServiceBlockingStub extends io.grpc.stub.AbstractStub<NameNodeServiceBlockingStub>
      implements NameNodeServiceBlockingClient {
    private NameNodeServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private NameNodeServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected NameNodeServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new NameNodeServiceBlockingStub(channel, callOptions);
    }

    @java.lang.Override
    public com.lvchao.dfs.namenode.rpc.model.RegisterResponse register(com.lvchao.dfs.namenode.rpc.model.RegisterRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_REGISTER, getCallOptions(), request);
    }

    @java.lang.Override
    public com.lvchao.dfs.namenode.rpc.model.HeartbeatResponse heartbeat(com.lvchao.dfs.namenode.rpc.model.HeartbeatRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_HEARTBEAT, getCallOptions(), request);
    }

    @java.lang.Override
    public com.lvchao.dfs.namenode.rpc.model.MkdirResponse mkdir(com.lvchao.dfs.namenode.rpc.model.MkdirRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_MKDIR, getCallOptions(), request);
    }

    @java.lang.Override
    public com.lvchao.dfs.namenode.rpc.model.ShutdownResponse shutdown(com.lvchao.dfs.namenode.rpc.model.ShutdownRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_SHUTDOWN, getCallOptions(), request);
    }

    @java.lang.Override
    public com.lvchao.dfs.namenode.rpc.model.FetchEditsLogResponse fetchEditsLog(com.lvchao.dfs.namenode.rpc.model.FetchEditsLogRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_FETCH_EDITS_LOG, getCallOptions(), request);
    }

    @java.lang.Override
    public com.lvchao.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse updateCheckpointTxid(com.lvchao.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_UPDATE_CHECKPOINT_TXID, getCallOptions(), request);
    }

    @java.lang.Override
    public com.lvchao.dfs.namenode.rpc.model.CreateFileResponse create(com.lvchao.dfs.namenode.rpc.model.CreateFileRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CREATE, getCallOptions(), request);
    }

    @java.lang.Override
    public com.lvchao.dfs.namenode.rpc.model.AllocateDataNodesResponse allocateDataNodes(com.lvchao.dfs.namenode.rpc.model.AllocateDataNodesRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_ALLOCATE_DATA_NODES, getCallOptions(), request);
    }

    @java.lang.Override
    public com.lvchao.dfs.namenode.rpc.model.InformReplicaReceivedResponse informReplicaReceived(com.lvchao.dfs.namenode.rpc.model.InformReplicaReceivedRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_INFORM_REPLICA_RECEIVED, getCallOptions(), request);
    }

    @java.lang.Override
    public com.lvchao.dfs.namenode.rpc.model.ReportCompleteStorageInfoResponse reportCompleteStorageInfo(com.lvchao.dfs.namenode.rpc.model.ReportCompleteStorageInfoRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_REPORT_COMPLETE_STORAGE_INFO, getCallOptions(), request);
    }

    @java.lang.Override
    public com.lvchao.dfs.namenode.rpc.model.GetDataNodeForFileResponse getDataNodeForFile(com.lvchao.dfs.namenode.rpc.model.GetDataNodeForFileRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_DATA_NODE_FOR_FILE, getCallOptions(), request);
    }
  }

  public static class NameNodeServiceFutureStub extends io.grpc.stub.AbstractStub<NameNodeServiceFutureStub>
      implements NameNodeServiceFutureClient {
    private NameNodeServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private NameNodeServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected NameNodeServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new NameNodeServiceFutureStub(channel, callOptions);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.lvchao.dfs.namenode.rpc.model.RegisterResponse> register(
        com.lvchao.dfs.namenode.rpc.model.RegisterRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_REGISTER, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.lvchao.dfs.namenode.rpc.model.HeartbeatResponse> heartbeat(
        com.lvchao.dfs.namenode.rpc.model.HeartbeatRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_HEARTBEAT, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.lvchao.dfs.namenode.rpc.model.MkdirResponse> mkdir(
        com.lvchao.dfs.namenode.rpc.model.MkdirRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_MKDIR, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.lvchao.dfs.namenode.rpc.model.ShutdownResponse> shutdown(
        com.lvchao.dfs.namenode.rpc.model.ShutdownRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_SHUTDOWN, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.lvchao.dfs.namenode.rpc.model.FetchEditsLogResponse> fetchEditsLog(
        com.lvchao.dfs.namenode.rpc.model.FetchEditsLogRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_FETCH_EDITS_LOG, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.lvchao.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse> updateCheckpointTxid(
        com.lvchao.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_UPDATE_CHECKPOINT_TXID, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.lvchao.dfs.namenode.rpc.model.CreateFileResponse> create(
        com.lvchao.dfs.namenode.rpc.model.CreateFileRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CREATE, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.lvchao.dfs.namenode.rpc.model.AllocateDataNodesResponse> allocateDataNodes(
        com.lvchao.dfs.namenode.rpc.model.AllocateDataNodesRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_ALLOCATE_DATA_NODES, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.lvchao.dfs.namenode.rpc.model.InformReplicaReceivedResponse> informReplicaReceived(
        com.lvchao.dfs.namenode.rpc.model.InformReplicaReceivedRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_INFORM_REPLICA_RECEIVED, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.lvchao.dfs.namenode.rpc.model.ReportCompleteStorageInfoResponse> reportCompleteStorageInfo(
        com.lvchao.dfs.namenode.rpc.model.ReportCompleteStorageInfoRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_REPORT_COMPLETE_STORAGE_INFO, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.lvchao.dfs.namenode.rpc.model.GetDataNodeForFileResponse> getDataNodeForFile(
        com.lvchao.dfs.namenode.rpc.model.GetDataNodeForFileRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_DATA_NODE_FOR_FILE, getCallOptions()), request);
    }
  }

  private static final int METHODID_REGISTER = 0;
  private static final int METHODID_HEARTBEAT = 1;
  private static final int METHODID_MKDIR = 2;
  private static final int METHODID_SHUTDOWN = 3;
  private static final int METHODID_FETCH_EDITS_LOG = 4;
  private static final int METHODID_UPDATE_CHECKPOINT_TXID = 5;
  private static final int METHODID_CREATE = 6;
  private static final int METHODID_ALLOCATE_DATA_NODES = 7;
  private static final int METHODID_INFORM_REPLICA_RECEIVED = 8;
  private static final int METHODID_REPORT_COMPLETE_STORAGE_INFO = 9;
  private static final int METHODID_GET_DATA_NODE_FOR_FILE = 10;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final NameNodeService serviceImpl;
    private final int methodId;

    public MethodHandlers(NameNodeService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_REGISTER:
          serviceImpl.register((com.lvchao.dfs.namenode.rpc.model.RegisterRequest) request,
              (io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.RegisterResponse>) responseObserver);
          break;
        case METHODID_HEARTBEAT:
          serviceImpl.heartbeat((com.lvchao.dfs.namenode.rpc.model.HeartbeatRequest) request,
              (io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.HeartbeatResponse>) responseObserver);
          break;
        case METHODID_MKDIR:
          serviceImpl.mkdir((com.lvchao.dfs.namenode.rpc.model.MkdirRequest) request,
              (io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.MkdirResponse>) responseObserver);
          break;
        case METHODID_SHUTDOWN:
          serviceImpl.shutdown((com.lvchao.dfs.namenode.rpc.model.ShutdownRequest) request,
              (io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.ShutdownResponse>) responseObserver);
          break;
        case METHODID_FETCH_EDITS_LOG:
          serviceImpl.fetchEditsLog((com.lvchao.dfs.namenode.rpc.model.FetchEditsLogRequest) request,
              (io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.FetchEditsLogResponse>) responseObserver);
          break;
        case METHODID_UPDATE_CHECKPOINT_TXID:
          serviceImpl.updateCheckpointTxid((com.lvchao.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest) request,
              (io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse>) responseObserver);
          break;
        case METHODID_CREATE:
          serviceImpl.create((com.lvchao.dfs.namenode.rpc.model.CreateFileRequest) request,
              (io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.CreateFileResponse>) responseObserver);
          break;
        case METHODID_ALLOCATE_DATA_NODES:
          serviceImpl.allocateDataNodes((com.lvchao.dfs.namenode.rpc.model.AllocateDataNodesRequest) request,
              (io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.AllocateDataNodesResponse>) responseObserver);
          break;
        case METHODID_INFORM_REPLICA_RECEIVED:
          serviceImpl.informReplicaReceived((com.lvchao.dfs.namenode.rpc.model.InformReplicaReceivedRequest) request,
              (io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.InformReplicaReceivedResponse>) responseObserver);
          break;
        case METHODID_REPORT_COMPLETE_STORAGE_INFO:
          serviceImpl.reportCompleteStorageInfo((com.lvchao.dfs.namenode.rpc.model.ReportCompleteStorageInfoRequest) request,
              (io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.ReportCompleteStorageInfoResponse>) responseObserver);
          break;
        case METHODID_GET_DATA_NODE_FOR_FILE:
          serviceImpl.getDataNodeForFile((com.lvchao.dfs.namenode.rpc.model.GetDataNodeForFileRequest) request,
              (io.grpc.stub.StreamObserver<com.lvchao.dfs.namenode.rpc.model.GetDataNodeForFileResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static io.grpc.ServerServiceDefinition bindService(
      final NameNodeService serviceImpl) {
    return io.grpc.ServerServiceDefinition.builder(SERVICE_NAME)
        .addMethod(
          METHOD_REGISTER,
          asyncUnaryCall(
            new MethodHandlers<
              com.lvchao.dfs.namenode.rpc.model.RegisterRequest,
              com.lvchao.dfs.namenode.rpc.model.RegisterResponse>(
                serviceImpl, METHODID_REGISTER)))
        .addMethod(
          METHOD_HEARTBEAT,
          asyncUnaryCall(
            new MethodHandlers<
              com.lvchao.dfs.namenode.rpc.model.HeartbeatRequest,
              com.lvchao.dfs.namenode.rpc.model.HeartbeatResponse>(
                serviceImpl, METHODID_HEARTBEAT)))
        .addMethod(
          METHOD_MKDIR,
          asyncUnaryCall(
            new MethodHandlers<
              com.lvchao.dfs.namenode.rpc.model.MkdirRequest,
              com.lvchao.dfs.namenode.rpc.model.MkdirResponse>(
                serviceImpl, METHODID_MKDIR)))
        .addMethod(
          METHOD_SHUTDOWN,
          asyncUnaryCall(
            new MethodHandlers<
              com.lvchao.dfs.namenode.rpc.model.ShutdownRequest,
              com.lvchao.dfs.namenode.rpc.model.ShutdownResponse>(
                serviceImpl, METHODID_SHUTDOWN)))
        .addMethod(
          METHOD_FETCH_EDITS_LOG,
          asyncUnaryCall(
            new MethodHandlers<
              com.lvchao.dfs.namenode.rpc.model.FetchEditsLogRequest,
              com.lvchao.dfs.namenode.rpc.model.FetchEditsLogResponse>(
                serviceImpl, METHODID_FETCH_EDITS_LOG)))
        .addMethod(
          METHOD_UPDATE_CHECKPOINT_TXID,
          asyncUnaryCall(
            new MethodHandlers<
              com.lvchao.dfs.namenode.rpc.model.UpdateCheckpointTxidRequest,
              com.lvchao.dfs.namenode.rpc.model.UpdateCheckpointTxidResponse>(
                serviceImpl, METHODID_UPDATE_CHECKPOINT_TXID)))
        .addMethod(
          METHOD_CREATE,
          asyncUnaryCall(
            new MethodHandlers<
              com.lvchao.dfs.namenode.rpc.model.CreateFileRequest,
              com.lvchao.dfs.namenode.rpc.model.CreateFileResponse>(
                serviceImpl, METHODID_CREATE)))
        .addMethod(
          METHOD_ALLOCATE_DATA_NODES,
          asyncUnaryCall(
            new MethodHandlers<
              com.lvchao.dfs.namenode.rpc.model.AllocateDataNodesRequest,
              com.lvchao.dfs.namenode.rpc.model.AllocateDataNodesResponse>(
                serviceImpl, METHODID_ALLOCATE_DATA_NODES)))
        .addMethod(
          METHOD_INFORM_REPLICA_RECEIVED,
          asyncUnaryCall(
            new MethodHandlers<
              com.lvchao.dfs.namenode.rpc.model.InformReplicaReceivedRequest,
              com.lvchao.dfs.namenode.rpc.model.InformReplicaReceivedResponse>(
                serviceImpl, METHODID_INFORM_REPLICA_RECEIVED)))
        .addMethod(
          METHOD_REPORT_COMPLETE_STORAGE_INFO,
          asyncUnaryCall(
            new MethodHandlers<
              com.lvchao.dfs.namenode.rpc.model.ReportCompleteStorageInfoRequest,
              com.lvchao.dfs.namenode.rpc.model.ReportCompleteStorageInfoResponse>(
                serviceImpl, METHODID_REPORT_COMPLETE_STORAGE_INFO)))
        .addMethod(
          METHOD_GET_DATA_NODE_FOR_FILE,
          asyncUnaryCall(
            new MethodHandlers<
              com.lvchao.dfs.namenode.rpc.model.GetDataNodeForFileRequest,
              com.lvchao.dfs.namenode.rpc.model.GetDataNodeForFileResponse>(
                serviceImpl, METHODID_GET_DATA_NODE_FOR_FILE)))
        .build();
  }
}
