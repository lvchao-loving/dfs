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

    public static final String NAMENODE_HOSTNAME = "localhost";
    public static final Integer NAMENODE_PORT = 50070;

    public static final String DATANODE_HOSTNAME = "dfs-data-01";
    public static final String DATANODE_IP = "127.0.0.1";
    public static final Integer NIO_PORT = 9000;

    public static final String DATA_DIR = "F:\\tmp";
}
