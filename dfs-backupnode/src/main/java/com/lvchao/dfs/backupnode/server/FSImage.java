package com.lvchao.dfs.backupnode.server;

/**
 * @Title: FSImage
 * @Package: com.lvchao.dfs.backupnode.server
 * @Description:
 * @auther: chao.lv
 * @date: 2021/10/30 18:42
 * @version: V1.0
 */

public class FSImage {
    private Long maxTxid;
    private String fsimageJson;

    public FSImage() {
    }

    public FSImage(Long maxTxid, String fsimageJson) {
        this.maxTxid = maxTxid;
        this.fsimageJson = fsimageJson;
    }

    public Long getMaxTxid() {
        return maxTxid;
    }

    public String getFsimageJson() {
        return fsimageJson;
    }

    public void setMaxTxid(Long maxTxid) {
        this.maxTxid = maxTxid;
    }

    public void setFsimageJson(String fsimageJson) {
        this.fsimageJson = fsimageJson;
    }
}
