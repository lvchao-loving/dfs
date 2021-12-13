package com.lvchao.dfs.datanode.server;

/**
 * @Title: DataNodeConfig
 * @Package: com.lvchao.dfs.datanode.server
 * @Description: datanode配置类
 * @auther: chao.lv
 * @date: 2021/11/18 19:55
 * @version: V1.0
 */
public class DataNodeConfig {

    public static volatile String NAMENODE_HOSTNAME = "localhost";
    public Integer NAMENODE_PORT = 50070;

    public String DATANODE_HOSTNAME = "localhost";
    public String DATANODE_IP = "127.0.0.1";
    public Integer NIO_PORT = 9301;

    public String DATA_DIR = "F:\\tmp1";

}
