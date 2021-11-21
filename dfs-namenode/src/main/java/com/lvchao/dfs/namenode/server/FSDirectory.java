package com.lvchao.dfs.namenode.server;

import java.util.LinkedList;
import java.util.List;

/**
 * 负责管理内存中的文件目录树的核心组件
 */
public class FSDirectory {

    /**
     * 内存中的文件目录树，有父子层级关系的数据结构
     */
    private INode dirTree;

    public INode getDirTree() {
        return dirTree;
    }

    public void setDirTree(INode dirTree) {
        this.dirTree = dirTree;
    }

    public FSDirectory() {
        this.dirTree = new INode("/");
    }

    /**
     * 创建目录
     *
     * @param path 目录路径
     */
    public void mkdir(String path) {

        synchronized (dirTree) {
            String[] pathes = path.split("/");
            INode parent = dirTree;

            for (String splitedPath : pathes) {
                if (splitedPath.trim().equals("")) {
                    continue;
                }

                INode dir = findDirectory(parent, splitedPath);
                if (dir != null) {
                    parent = dir;
                    continue;
                }

                INode child = new INode(splitedPath);
                parent.addChild(child);
                parent = child;
            }
        }

        //printDirTree(dirTree, "");
    }

    /**
     * 文件目录树的打印
     *
     * @param dirTree
     * @param blank
     */
    public void printDirTree(INode dirTree, String blank) {
        if (dirTree.getChildren().size() == 0) {
            return;
        }
        for (INode dir : dirTree.getChildren()) {
            ThreadUntils.println(blank + ((INode) dir).getPath());
            printDirTree((INode) dir, blank + " ");
        }
    }

    /**
     * 对文件目录树递归查找目录
     *
     * @param dir
     * @param path
     * @return
     */
    private INode findDirectory(INode dir, String path) {
        if (dir.getChildren().size() == 0) {
            return null;
        }

        INode resultDir = null;

        for (INode child : dir.getChildren()) {
            if (child instanceof INode) {
                INode childDir = (INode) child;

                if ((childDir.getPath().equals(path))) {
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
            return "INode{" +
                    "path='" + path + '\'' +
                    ", children=" + children +
                    '}';
        }
    }

    /**
     * 创建文件
     *
     * @param filename 文件名称
     * @return
     */
    public Boolean create(String filename) {
        synchronized (dirTree) {
            String[] splitedFilename = filename.split("/");
            String realFilename = splitedFilename[splitedFilename.length - 1];

            INode parent = dirTree;

            for (int i = 0; i < splitedFilename.length - 1; i++) {
                // 略过以 / 开始的文件名称
                if ("/".equals(splitedFilename[i])) {
                    continue;
                }

                INode dir = findDirectory(parent, splitedFilename[i]);

                if (dir != null) {
                    parent = dir;
                    continue;
                }

                INode child = new INode(splitedFilename[i]);
                parent.addChild(child);
                parent = child;
            }

            if (existFile(parent, realFilename)) {
                return false;
            }

            INode file = new INode(realFilename);
            parent.addChild(file);

            System.out.println(dirTree);

            return true;
        }
    }

    /**
     * 目录下是否存在这个文件
     *
     * @param dir
     * @param filename
     * @return
     */
    private Boolean existFile(INode dir, String filename) {
        if (dir.getChildren() != null && dir.getChildren().size() > 0) {
            for (INode child : dir.getChildren()) {
                if (child.getPath().equals(filename)) {
                    return true;
                }
            }
        }
        return false;
    }
}
