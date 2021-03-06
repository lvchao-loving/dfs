package com.lvchao.dfs.backupnode.server;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @Title: FSDirectory
 * @Package: com.lvchao.dfs.backupnode.server
 * @Description: 负责管理内存中的文件目录树的核心组件
 * @auther: chao.lv
 * @date: 2021/10/26 15:04
 * @version: V1.0
 */
public class FSDirectory {

    /**
     * 内存中的文件目录树
     */
    private INode dirTree;

    /**
     * 记录当前 Inode 节点中对应操作的 txid
     */
    private Long maxTxid = 0L;
    /**
     * 文件目录树的读写锁
     */
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public Long getMaxTxid() {
        return maxTxid;
    }

    public void setMaxTxid(Long maxTxid) {
        this.maxTxid = maxTxid;
    }

    public INode getDirTree() {
        return dirTree;
    }

    public void setDirTree(INode dirTree) {
        this.dirTree = dirTree;
    }

    public void writeLock(){
        lock.writeLock().lock();
    }

    public void writeUnLock(){
        lock.writeLock().unlock();
    }

    public void readLock(){
        lock.readLock().lock();
    }

    public void readUnLock(){
        lock.readLock().unlock();
    }

    public FSDirectory() {
        // 默认刚开始就是空的节点
        this.dirTree = new INode("/");
    }

    /**
     * 把 内存目录树转成 FSImage 对象
     * @return
     */
    public FSImage getFSImage(){
        FSImage fsImage = null;
        try{
            readLock();
            String fsImageJson = JSONObject.toJSONString(dirTree);
            fsImage = new FSImage(maxTxid,fsImageJson);
        }finally {
            readUnLock();
        }
        return fsImage;
    }

    /**
     * 利用 ReentrantLock的写锁，将目录树暂存到内存中；同时设置 maxTxid
     * 创建目录
     * @param path
     */
    public void mkdir(Long txid,String path) {
        // 内存数据结构，更新的时候必须得加锁的
        try {
            writeLock();

            maxTxid = txid;

            String[] pathes = path.split("/");
            INode parent = dirTree;

            for(String splitedPath : pathes) {
                if(splitedPath.trim().equals("")) {
                    continue;
                }

                INode dir = findDirectory(parent, splitedPath);

                if(dir != null) {
                    parent = dir;
                    continue;
                }

                INode child = new INode(splitedPath);
                parent.addChild(child);
                parent = child;
            }
        }finally {
            writeUnLock();
        }

        // printDirTree(dirTree, "");
    }

    /**
     * 创建文件
     * @param txid
     * @param filename
     * @return
     */
    public Boolean create(Long txid , String filename) {
        // 保证多线程之间的安全性
        try {
            writeLock();

            maxTxid = txid;

            String[] split = filename.split("/");
            String readFilename = split[split.length - 1];

            // 添加文件目录树
            INode parent = dirTree;
            for (int i = 0; i < split.length - 1; i++) {
                if (StringUtils.isBlank(split[i]) || "/".equals(split[i])){
                    continue;
                }

                INode dir = findDirectory(parent,split[i]);
                if (dir != null){
                    parent = dir;
                    continue;
                }

                INode child = new INode(split[i]);
                parent.addChild(child);
                parent = child;
            }

            // 判断文件是否已存在
            if (existFile(parent,readFilename)){
                return false;
            }

            // 在文件目录数中添加一个文件
            INode file = new INode(readFilename);
            parent.addChild(file);
            return true;

        }finally {
            writeUnLock();
        }
    }

    /**
     * 判断目录下是否存在指定名称的文件
     * @return
     */
    private Boolean existFile(INode dir, String filename){
        if (dir.getChildren() != null && dir.getChildren().size() > 0){
            for (INode child:dir.getChildren()){
                if (child.getPath().equals(filename)){
                    return true;
                }
            }
        }
        return false;
    }


    /**
     * 打印创建的目录
     * @param dirTree
     * @param blank
     */
    private void printDirTree(INode dirTree, String blank) {
        if(dirTree.getChildren().size() == 0) {
            return;
        }
        for(INode dir : dirTree.getChildren()) {
            ThreadUntils.println(blank + ((INode) dir).getPath());
            printDirTree((INode) dir, blank + " ");
        }
    }

    /**
     * 查找子目录
     * @param dir
     * @param path
     * @return
     */
    private INode findDirectory(INode dir, String path) {
        if(dir.getChildren().size() == 0) {
            return null;
        }

        for(INode child : dir.getChildren()) {
            if(child instanceof INode) {
                INode childDir = (INode) child;
                if((childDir.getPath().equals(path))) {
                    return childDir;
                }
            }
        }

        return null;
    }

    /**
     * 代表文件目录树中的一个目录
     */
    public static class INode {

        private String path;
        private List<INode> children;

        public INode() {
        }

        public INode(String path) {
            this.path = path;
            this.children = new LinkedList<INode>();
        }

        public void addChild(INode inode) {
            this.children.add(inode);
        }

        public String getPath() {
            return path;
        }
        public void setPath(String path) {
            this.path = path;
        }
        public List<INode> getChildren() {
            return children;
        }
        public void setChildren(List<INode> children) {
            this.children = children;
        }

        @Override
        public String toString() {
            return "INode [path=" + path + ", children=" + children + "]";
        }

    }
}

