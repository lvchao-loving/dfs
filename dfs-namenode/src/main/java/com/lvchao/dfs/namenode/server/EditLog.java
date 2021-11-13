package com.lvchao.dfs.namenode.server;

import com.alibaba.fastjson.JSONObject;

/**
 * @Title: EditLog
 * @Package: com.lvchao.dfs.namenode.server
 * @Description: 代表了一条edits log
 * @auther: chao.lv
 * @date: 2021/10/25 13:40
 * @version: V1.0
 */
public class EditLog {
    private long txid;
    private String content;

    public EditLog(long txid, String content) {
        this.txid = txid;
        JSONObject jsonObject = JSONObject.parseObject(content);
        jsonObject.put("txid",txid);

        this.content = jsonObject.toJSONString();
    }

    public long getTxid() {
        return txid;
    }

    public String getContent() {
        return content;
    }

    public void setTxid(long txid) {
        this.txid = txid;


    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public String toString() {
        return "EditLog{" +
                "txid=" + txid +
                ", content='" + content + '\'' +
                '}';
    }
}
