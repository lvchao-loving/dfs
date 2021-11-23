package com.lvchao.dfs.namenode.server;

/**
 * @Title: Command
 * @Package: com.lvchao.dfs.namenode.server
 * @Description: 下发给 DataNode的命令
 * @auther: chao.lv
 * @date: 2021/11/23 21:50
 * @version: V1.0
 */
public class Command {
    public static final Integer REGISTER = 1;
    public static final Integer REPORT_COMPLETE_STORAGE_INFO = 2;

    private Integer type;
    private String content;

    public Command(){}

    public Command(Integer type ){
        this.type = type;
    }

    public Integer getType() {
        return type;
    }

    public String getContent() {
        return content;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public String toString() {
        return "Command{" +
                "type=" + type +
                ", content='" + content + '\'' +
                '}';
    }
}
