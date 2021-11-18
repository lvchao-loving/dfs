// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: NameNodeRpcServer.proto

package com.lvchao.dfs.namenode.rpc.service;

public final class NameNodeServer {
  private NameNodeServer() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\027NameNodeRpcServer.proto\022\033com.lvchao.df" +
      "s.namenode.rpc\032\026NameNodeRpcModel.proto2\245" +
      "\007\n\017NameNodeService\022g\n\010register\022,.com.lvc" +
      "hao.dfs.namenode.rpc.RegisterRequest\032-.c" +
      "om.lvchao.dfs.namenode.rpc.RegisterRespo" +
      "nse\022j\n\theartbeat\022-.com.lvchao.dfs.nameno" +
      "de.rpc.HeartbeatRequest\032..com.lvchao.dfs" +
      ".namenode.rpc.HeartbeatResponse\022^\n\005Mkdir" +
      "\022).com.lvchao.dfs.namenode.rpc.MkdirRequ" +
      "est\032*.com.lvchao.dfs.namenode.rpc.MkdirR",
      "esponse\022g\n\010shutdown\022,.com.lvchao.dfs.nam" +
      "enode.rpc.ShutdownRequest\032-.com.lvchao.d" +
      "fs.namenode.rpc.ShutdownResponse\022v\n\rfetc" +
      "hEditsLog\0221.com.lvchao.dfs.namenode.rpc." +
      "FetchEditsLogRequest\0322.com.lvchao.dfs.na" +
      "menode.rpc.FetchEditsLogResponse\022\213\001\n\024upd" +
      "ateCheckpointTxid\0228.com.lvchao.dfs.namen" +
      "ode.rpc.UpdateCheckpointTxidRequest\0329.co" +
      "m.lvchao.dfs.namenode.rpc.UpdateCheckpoi" +
      "ntTxidResponse\022i\n\006create\022..com.lvchao.df",
      "s.namenode.rpc.CreateFileRequest\032/.com.l" +
      "vchao.dfs.namenode.rpc.CreateFileRespons" +
      "e\022\202\001\n\021allocateDataNodes\0225.com.lvchao.dfs" +
      ".namenode.rpc.AllocateDataNodesRequest\0326" +
      ".com.lvchao.dfs.namenode.rpc.AllocateDat" +
      "aNodesResponseB7\n#com.lvchao.dfs.namenod" +
      "e.rpc.serviceB\016NameNodeServerP\001b\006proto3"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
        new com.google.protobuf.Descriptors.FileDescriptor.    InternalDescriptorAssigner() {
          public com.google.protobuf.ExtensionRegistry assignDescriptors(
              com.google.protobuf.Descriptors.FileDescriptor root) {
            descriptor = root;
            return null;
          }
        };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
          com.lvchao.dfs.namenode.rpc.model.NameNodeRpcModel.getDescriptor(),
        }, assigner);
    com.lvchao.dfs.namenode.rpc.model.NameNodeRpcModel.getDescriptor();
  }

  // @@protoc_insertion_point(outer_class_scope)
}
