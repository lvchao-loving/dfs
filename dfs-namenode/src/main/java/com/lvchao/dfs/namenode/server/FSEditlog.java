package com.lvchao.dfs.namenode.server;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * 负责管理edits log日志的核心组件
 */
public class FSEditlog {

	/**
	 * EditsLog 日志文件清理时间毫秒
	 */
	private static final Long EDIT_LOGCLEAN_Interval = 30 * 1000L;

	private FSNamesystem fsNamesystem;
	/**
	 * 当前递增到的txid的序号
	 */
	private long txidSeq = 0L;
	/**
	 * 内存双缓冲区
	 */
	private DoubleBuffer doubleBuffer = new DoubleBuffer();
	/**
	 * 当前是否在将内存缓冲刷入磁盘中
	 */
	private volatile Boolean isSyncRunning = false;
	/**
	 * 在同步到磁盘中的最大的一个txid
	 */
	private volatile Long syncTxid = 0L;
	/**
	 * 是否正在调度一次刷盘的操作
	 */
	private volatile Boolean isSchedulingSync = false;
	/**
	 * 每个线程自己本地的txid副本
	 */
	private ThreadLocal<Long> localTxid = new ThreadLocal<Long>();

	/**
	 * 存储持久化到磁盘的配置的记录，升序排列
	 */
	private List<EditslogInfo> editslogInfoList = new ArrayList<>();
	// 就会导致说，对一个共享的map数据结构出现多线程并发的读写的问题
	// 此时对这个map的读写是不是就需要加锁了
	//	private Map<Thread, Long> txidMap = new HashMap<Thread, Long>();

	public List<EditslogInfo> getEditslogInfoList() {
		return editslogInfoList;
	}


	public FSEditlog(FSNamesystem fsNamesystem){
		this.fsNamesystem = fsNamesystem;
		// 启动初始本地 EditsLog 磁盘文件，根据 BackupNode 发送过来的最大 txid 删除之前的磁盘文件
		EditLogClean editLogClean = new EditLogClean();
		editLogClean.setName("EditLogClean");
		editLogClean.start();
	}

	/**
	 * 记录edits log日志
	 * @param content
	 */
	public void logEdit(String content) {
		// 加锁，保证线程安全
		synchronized(this) {
			// 检查一下是否正在调度一次刷盘的操作
			waitSchedulingSync();

			// 获取全局唯一递增的txid，代表了edits log的序号
			txidSeq++;
			long txid = txidSeq;
			// 放到ThreadLocal里去，相当于就是维护了一份本地线程的副本
			localTxid.set(txid);
			// 构造一条edits log对象
			EditLog log = new EditLog(txid, content);
			// 将edits log写入内存缓冲中，不是直接刷入磁盘文件
			try {
				doubleBuffer.write(log);
			} catch (IOException e) {
				e.printStackTrace();
			}

			// 每次写完一条editslog之后，就应该检查一下当前这个缓冲区是否满了
			if(!doubleBuffer.shouldSyncToDisk()) {
				return;
			}

			// 如果代码进行到这里，就说明需要刷磁盘
			isSchedulingSync = true;
		}

		// 释放掉锁
		logSync();
	}

	/**
	 * 等待正在调度的刷磁盘的操作
	 */
	private void waitSchedulingSync() {
		try {
			while(isSchedulingSync) {
				wait(1000);
				// 此时就释放锁，等待一秒再次尝试获取锁，去判断
				// isSchedulingSync是否为false，就可以脱离出while循环
			}
		} catch (Exception e) {
			//log.info("waitSchedulingSync has interrupted !!");
			e.printStackTrace();
		}
	}

	/**
	 * 将内存缓冲中的数据刷入磁盘文件中
	 * 在这里尝试允许某一个线程一次性将内存缓冲中的数据刷入磁盘文件中
	 * 相当于实现一个批量将内存缓冲数据刷磁盘的过程
	 */
	private void logSync() {
		// 再次尝试加锁
		synchronized(this) {
			// 获取到本地线程中获取txid
			long txid = localTxid.get();

			// 当第一块正在刷新磁盘中还没有完成时，但是第二块已经写满了，则将经此下面if判断并等待中
			if(isSyncRunning) {
				if(txid <= syncTxid) {
					return;
				}
				try {
					while(isSyncRunning) {
						wait(1000);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			// 交换两块缓冲区
			doubleBuffer.setReadyToSync();
			// 记录当前磁盘同步最大 txid
			syncTxid = txid;
			// 设置当前正在同步到磁盘的标志位
			isSchedulingSync = false;
			// 唤醒那些还卡在while循环那儿的线程
			notifyAll();
			isSyncRunning = true;
		}

		// 开始同步内存缓冲的数据到磁盘文件里去
		// 这个过程其实是比较慢，基本上肯定是毫秒级了，弄不好就要几十毫秒
		try {
			doubleBuffer.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

		synchronized(this) {
			// 同步完了磁盘之后，就会将标志位复位，再释放锁
			isSyncRunning = false;
			// 唤醒可能正在等待他同步完磁盘的线程
			notifyAll();
		}
	}

	/**
	 * 强制把内存缓冲里的数据刷入磁盘中
	 */
    public void flush() {
		try {
			// 因为 syncBuffer负责同步磁盘，currentBuffer负责同步磁盘，未同步到磁盘的数据还保留在currentBuffer中，所以需要交换一下
			doubleBuffer.setReadyToSync();
			doubleBuffer.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }

	/**
	 * 从 FSEditlog 中获取 doubleBuffer 中的 flushedTxids
	 * @return
	 */
	public List<String> getFlushedTxids(){
		return doubleBuffer.getFlushedTxids();
	}

	/**
	 * 获取当前缓冲区里的数据
	 */
	public String[] getBufferedEditsLog() {
		synchronized(this) {
			return doubleBuffer.getBufferedEditsLog();
		}
	}

	class EditLogClean extends Thread{
		@Override
		public void run() {
			while (true){
				try {
					// fsNamesystem.getCheckpointTxid();
					TimeUnit.MILLISECONDS.sleep(EDIT_LOGCLEAN_Interval);
					ThreadUntils.println("EditLogClean线程睡眠..." + EDIT_LOGCLEAN_Interval + "毫秒");
					List<String> flushedTxids = getFlushedTxids();
					if (flushedTxids != null && flushedTxids.size() > 0){
						Long checkpointTxid = fsNamesystem.getCheckpointTxid();
						for (String flushedTxid:flushedTxids) {
							String[] txids = flushedTxid.split("_");
							Long startTxid = Long.valueOf(txids[0]);
							Long endTxid = Long.valueOf(txids[1]);
							if (checkpointTxid >= endTxid){
								// 删除文件
								String filePath = "F:\\editslog\\edits-" + startTxid + "-" + endTxid + ".log";
								File file = new File(filePath);
								if (file.exists()){
									ThreadUntils.println("发现editlog日志文件不需要 ，进行删除" + filePath);
									file.delete();
								}
							}
						}
					}
				}catch (Exception e){
					e.printStackTrace();
				}
			}
		}
	}
}