package com.lvchao.dfs.namenode.server;

import java.util.Objects;

/**
 * @Title: EditslogInfo
 * @Package: com.lvchao.dfs.namenode.server
 * @Description: editslog文件信息
 * @auther: chao.lv
 * @date: 2021/10/27 21:59
 * @version: V1.0
 */
public class EditslogInfo implements Comparable<EditslogInfo> {
    private long start;
    private long end;
    private String name;

    @Override
    public int compareTo(EditslogInfo o) {
        return (int) (this.start - o.start);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EditslogInfo that = (EditslogInfo) o;
        return start == that.start &&
                end == that.end &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end, name);
    }
}
