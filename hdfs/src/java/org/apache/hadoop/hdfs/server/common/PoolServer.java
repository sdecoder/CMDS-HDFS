package org.apache.hadoop.hdfs.server.common;

import java.io.IOException;
import java.io.FileOutputStream;
import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.ServerSocketChannel;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.io.File;
import java.io.RandomAccessFile;
import java.io.EOFException;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.commons.logging.Log;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.PoolFile.*;
import org.apache.hadoop.hdfs.server.datanode.ReplicaPoolInfo;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLog.OP_INVALID;
import static org.apache.hadoop.hdfs.server.namenode.FSEditLog.OP_WAL;


/********************************************************
 * PoolServer is used as socket server and RPC server to 
 * response clients. It also implements PoolProtocol 
 * interfaces.
 *
 ********************************************************/

public class PoolServer implements PoolProtocol, FSConstants {
	public static final Log LOG = LogFactory.getLog(PoolServer.class);
		
	private Configuration conf;
	volatile boolean shouldRun = true;
	
	//for create listen socket
	ThreadGroup threadGroup = null;
	int socketWriteTimeout = 0;
	int socketTimeout = 0;
	Daemon poolXceiverServer = null;
	StorageClover storage;
	
	//for RPC server
	Server rpcServer;
	
	//for keep replica file info	
	HashMap<String, ReplicaPoolInfo> map = new 
	HashMap<String, ReplicaPoolInfo>();
	
	//keep editlogfileinput stream
	HashMap<String, EditLogFileInputStream> logInputMap = new
	HashMap<String, EditLogFileInputStream>();
	
	//keep editlogfileoutput stream
	HashMap<String, EditLogFileOutputStream> logOutputMap = new
	HashMap<String, EditLogFileOutputStream>();

	public static HashMap<String, String> walTransMap = new
	HashMap<String, String>();
	boolean isWalLoad = false;
	//trans array for rpc transfer
	List<String> trans = new ArrayList<String>();	
	
	int wirtePacketSize = 0;
	  /** Header size for a packet */
	final int PKT_HEADER_LEN = ( 4 + /* Packet payload length */
							8 + /* offset in block */
	                        8 + /* seqno */
	                        1   /* isLastPacketInBlock */);
	
	PoolServer(StorageClover storage) throws IOException {
		this.storage = storage;
		conf = new Configuration();
		this.wirtePacketSize = conf.getInt(DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, 
                                       DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT);
			
		//create RPC server
		InetSocketAddress rpcAddr = NetUtils.createSocketAddr(
				conf.get("dfs.poolnode.ipc.address", "0.0.0.0:60021"));
		int handlerCount = conf.getInt("dfs.namenode.handler.count", 10);
		rpcServer = RPC.getServer(PoolProtocol.class, this, rpcAddr.getHostName(), rpcAddr.getPort(), 
						handlerCount, false, conf, null);
		rpcServer.start();	
		////LOG.info("rpcServer has started at hostname: " + rpcAddr.getHostName() + ", port:" + rpcAddr.getPort());

	
		// create threadGroup and listen socket
		InetSocketAddress socAddr = NetUtils.createSocketAddr(
					conf.get("dfs.poolnode.address", "0.0.0.0:60011"));
		int port = socAddr.getPort();
		socketWriteTimeout = conf.getInt("dfs.namenode.socket.write.timeout", 
					HdfsConstants.WRITE_TIMEOUT);
	    	socketTimeout =  conf.getInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY,
                HdfsConstants.READ_TIMEOUT);
		//create listen socket
		ServerSocket ss = (socketWriteTimeout > 0) ?
				ServerSocketChannel.open().socket() : new ServerSocket();
		Server.bind(ss, socAddr, 0);		
		ss.setReceiveBufferSize(DEFAULT_DATA_SOCKET_SIZE);
		////LOG.info("bind server listen socket at " + 
		//			socAddr.getHostName() + " : " + port);
		
		//create thread group
		this.threadGroup = new ThreadGroup("PoolXceiverServer");
		this.poolXceiverServer = new Daemon(threadGroup, 
				new PoolXceiverServer(ss, conf, this));
		this.threadGroup.setDaemon(true); //auto destroy when empty
		poolXceiverServer.start();
		////LOG.info("poolXceiverServer has started");
		
	}
	
	/**
	 * Current system time.
	 */
	static long now() {
		return System.currentTimeMillis();
	}
	
	int getXceiverCount() {
		    return threadGroup == null ? 0 : threadGroup.activeCount();
	}
	
	public void join() {
		try {
			this.rpcServer.join();			
		} catch (InterruptedException ie) {
			
		}
	}
		
	public long getProtocolVersion(String protocol, 
			long clientVersion) throws IOException {
		if(protocol.equals(PoolProtocol.class.getName())) {
			return PoolProtocol.versionID;
		}else {
			throw new IOException("Unknown protocol to name node:" + protocol);
		}		
	}
	
	/**
	 * implement PoolProtocol interface, must be synchronized
	 * construct logical storage directory/file view
	 * each poolfile include name(not include root), nodelist and crc 
	 */
	public int fileReport(String clientName, int rpcPort, int socketPort, PoolFileInfo[] files)
		throws IOException {
		////LOG.info("enter file report");	
		
	
		String strname;
		PoolFile pf = new PoolFile();
		Map<String, PoolFile> poolFiles;
		PoolFile tmppf;
		NameNodeInfo nodeinfo;
		boolean needAdd = true;
		////LOG.info("call rpc to file report");	
	
		synchronized (this) {
			//handle file list from pool client
			for (PoolFileInfo info : files) {
				nodeinfo = pf.new NameNodeInfo(clientName, rpcPort, socketPort);
				nodeinfo.setCrc(info.getCrc());
				
				strname = info.getNodeDirName();
				Map<String, PoolDirectory> mapd = storage.dirMap.get(strname);
				if (mapd == null) {
					////LOG.info("add mapd: " + strname);
					mapd = new HashMap<String, PoolDirectory>();
					storage.dirMap.put(strname, mapd);
				}		 
				assert mapd != null : "not directory, like node1";
				strname = info.getDirName();			 
				PoolDirectory pd = mapd.get(strname);
				if (pd == null) {
					////LOG.info("add pool directory: " + strname);	
					pd = pf.new PoolDirectory(strname);
					mapd.put(strname, pd);
				}
				assert pd != null : "not file directory, like current";
				strname = info.getFilePathName();
				poolFiles = pd.getPoolFiles();
				tmppf = poolFiles.get(strname);
				if(tmppf == null) {
					////LOG.info("add pool file: " + strname);	
					tmppf = new PoolFile(strname, nodeinfo);
					poolFiles.put(strname, tmppf);
				}else {
					for (NameNodeInfo node : tmppf.getNodeInfos()) {
						if (node.getHostName().equals( clientName)) {
							needAdd = false;
							break;
						}
					}
					
					if (needAdd) {
						tmppf.addChild(nodeinfo);
						////LOG.info("add node info to pool file: " + strname + ", node num: " + tmppf.getNodeInfos().size());	
					}
				}		 				
			}
		}
		
		return 0;		
	}
	
	//rename file or directoriy
	public void rename(PoolFile from, PoolFile to, String clientName) 
		throws IOException
	{
		////LOG.info("call rpc to rename, from:" + from.getName() + 
		//		" to:" + to.getName());
		//first operate in mapDir
		String fromDir = from.getDirName();
		String toDir = to.getDirName();
		String tmpName = "";
		File fromFile = from.getLocalFile();
		File toFile = to.getLocalFile();
		
		Map<String, PoolDirectory> mapd = StorageClover.dirMap.get(clientName);
		assert mapd != null : "no hostname directory, like node1";
		PoolDirectory pd = mapd.get(fromDir);
		assert pd != null : "no file directory, like current";
		if (!from.isDir){
			//modify dirMap for file
			Map<String, PoolFile> poolFiles = pd.getPoolFiles();
			PoolFile pf = poolFiles.get(from.getName());
			assert pf !=null : "no file in dirMap";
			PoolFile pfto = poolFiles.get(to.getName());
			if (pfto != null) {
				poolFiles.remove(from.getName());
			} else {
				poolFiles.remove(from.getName());
				pf.setName(to.getName()); //use original namenode list
				poolFiles.put(to.getName(), pf);
			}
		} else {
			//the new directory must be empty
			//modify dirMap for directory, check checkpoint directory
			mapd.remove(fromDir);
			pd.setDirName(toDir);
			mapd.put(toDir, pd);
			if (to.getName().indexOf("checkpoints") >= 0) {
				if (!toFile.exists())
					toFile.mkdirs();
				//need to modify file path in dirMap
				//from node/tmp/fsimage -> node/checkpointXX/node/current/fsiamge
				Map<String, PoolFile> poolFiles = pd.getPoolFiles();
				List<String> keysets = new ArrayList<String>();				
				keysets.addAll(poolFiles.keySet());
				for (String pfName : keysets) {
					PoolFile pf = poolFiles.get(pfName);
					tmpName = pf.getLastName();
					pf.setName(clientName + "/" + to.getDirName() +
							"/" + clientName + "/current/" + tmpName);
					poolFiles.remove(pfName);
					poolFiles.put(pf.getName(), pf);
					////LOG.info("poolfiles put:" + pf.getName());					
				}
			}
		}
		
		//then rename in local node if exists
		if(fromFile.exists()){
			if(!fromFile.renameTo(toFile))
				throw new IOException("Failed to rename " 
						+ fromFile.getCanonicalPath() + " to " + toFile.getCanonicalPath());
		}
	}
	
	//delete the directory
	public void fullDelete(PoolFile dir) throws IOException {
		//first operate in mapDir
		String name = dir.getNodeDirName();
		File dirFile = null;
		
		Map<String, PoolDirectory> mapd = StorageClover.dirMap.get(name);
		if (mapd != null) {
			name = dir.getDirName();
			PoolDirectory pd = mapd.get(name);
			if (pd != null) {
				mapd.remove(name);
			}
		}
		
		//then delete in local node if directory exists
		dirFile = dir.getLocalFile();
		////LOG.info("first full delete dir: " + dirFile.getPath());
		if(dirFile.exists()) {
			if(!FileUtil.fullyDelete(dirFile))
				throw new IOException("Failed to delete" + dirFile.getCanonicalPath());
		}		
	}
	
	//delete the file
	public boolean delete(PoolFile file){
		//first get the file in mapDir
		String name = file.getNodeDirName();
		
		Map<String, PoolDirectory> mapd = StorageClover.dirMap.get(name);
		assert mapd != null : "not hostname directory, like node1";
		name = file.getDirName();
		PoolDirectory pd = mapd.get(name);
		assert pd != null : "not file directory, like current";
		Map<String, PoolFile> poolFiles = pd.getPoolFiles();
		PoolFile pf = poolFiles.get(file.getName());
		if (pf == null){
			return false; 
		}
		else {
			poolFiles.remove(file.getName());
			return file.getLocalFile().delete();
		}
	}
	
	//create ${currentDir}
	public void mkdir(PoolFile dir) {
		//modify dirMap in memory
		PoolFile pf = new PoolFile();
		String name = dir.getNodeDirName();
		Map<String, PoolDirectory> mapd = StorageClover.dirMap.get(name);
		if (mapd == null) {
			////LOG.info("call rpc to mkdir, add node name to mapd: " + name);
			mapd = new HashMap<String, PoolDirectory>();
			StorageClover.dirMap.put(name, mapd);
		}
		name = dir.getDirName();
		PoolDirectory pd = mapd.get(name);
		if (pd == null){
			////LOG.info("call rpc to mkdir, add dir name to mapd: " + name);
			pd = pf.new PoolDirectory(name);
			mapd.put(name, pd);
		}
		//mkdir 
		File d = dir.getLocalFile();
		////LOG.info("call rpc to mkdirs locally : " + d.getPath());
		d.mkdirs();
	}
		
	//wrtie /image/fsimage
	public void writeCorruptedData(PoolFile file)
				throws IOException {
		
		file.createdirs();
		RandomAccessFile oldfile = new RandomAccessFile(file.getLocalFile(), "rws");
		////LOG.info("call rpc to writecorrupteddata:" + file.getLocalFile().getPath());
		final String messageForPreUpgradeVersion =
			"\nThis file is INTENTIONALLY CORRUPTED so that versions\n"
		    + "of Hadoop prior to 0.13 (which are incompatible\n"
		    + "with this directory layout) will fail to start.\n";		
		try{
			oldfile.seek(0);
			oldfile.writeInt(FSConstants.LAYOUT_VERSION);
			org.apache.hadoop.hdfs.DeprecatedUTF8.writeString(oldfile, "");
			oldfile.writeBytes(messageForPreUpgradeVersion);
			oldfile.getFD().sync();
		} finally {
			oldfile.close();
		}
		
	}
	
	//write fstime and version file
	public void writeVersion(PoolFile to, String[] values, long ckpTime)
			throws IOException {
		to.createdirs();
		Properties props = new Properties();
		props.setProperty("layoutVersion", values[0]);
		props.setProperty("storageType", values[1]);
		props.setProperty("namespaceID", values[2]);
		props.setProperty("cTime", values[3]);
		if (values[4] != null) {
			////LOG.info("set distributed props: " + values[4]);
			props.setProperty("distributedUpgradeState", values[4]);
			props.setProperty("distributedUpgradeVersion", values[5]);
		}
		// first write version file
		RandomAccessFile file = new RandomAccessFile(to.getLocalFile(), "rws");
		////LOG.info("call rpc to write version : " + to.getLocalFile());
		FileOutputStream out = null;
		try {
			file.seek(0);
			out = new FileOutputStream(file.getFD());
			props.store(out, null);
			file.setLength(out.getChannel().position());
		} finally {
			if (out != null)
				out.close();
			file.close();
		}
		
		//then write las chekpoint time to fstime file
		if (ckpTime < 0L) {
			////LOG.info("do not write negative time");
			return; 	
		}		
		String name = to.getRootpath() + "/" + to.getNodeDirName() +
			"/" + to.getDirName() + "/fstime";
		File timeFile = new File(name);
		////LOG.info("call rpc to write checkpointtime: " + name);
		if (timeFile.exists() && !timeFile.delete()) {
			LOG.error("Cannot delete chekpoint time file: " + timeFile.getCanonicalPath());
		}
		DataOutputStream stream = new DataOutputStream(
			new FileOutputStream(timeFile));
		try {
			stream.writeLong(ckpTime);
		} finally {
			stream.close();
		}
	}
	
	//read version file
	public Properties readVersion(PoolFile from) 
			throws IOException {
		RandomAccessFile file = new RandomAccessFile(from.getLocalFile(), "rws");
		FileInputStream in = null;
		Properties props = null;
		try {
			in = new FileInputStream(file.getFD());
			file.seek(0);
			props = new Properties();
			props.load(in);
		} catch (IOException ie) {
			if (in != null)
				in.close();
			file.close();
			//LOG.info("read version failed:" + from.getLocalFile());
			return null;
		} finally {
			if (in != null)
				in.close();
			file.close();
		}
		return props;
	}

	/**
	 * Shut down this instance of the poolserver
	 */
	public void shutdown() {
		if (rpcServer != null) {
		  rpcServer.stop();
		  //LOG.info("stop rpcserver: " + rpcServer);	
		}		
		this.shouldRun = false;
		if (poolXceiverServer != null) {
		  ((PoolXceiverServer) this.poolXceiverServer.getRunnable()).kill();
		  this.poolXceiverServer.interrupt();

		  //wait for all data receiver threads to exit
		  if (this.threadGroup != null) {
			int sleepMs = 2;
			while (true) {
			  this.threadGroup.interrupt();
			  //LOG.info("Waiting for threadgroup to exit, active thread :" + 
				//this.threadGroup.activeCount());
			  if (this.threadGroup.activeCount() == 0 ) {
			  	//LOG.info("threadgroup active count is 0");
			        break;
			  }
			  try {
			  	Thread.sleep(sleepMs);
			  } catch (InterruptedException e) {}
			  sleepMs = sleepMs * 3 / 2; //exponential backoff
			  if (sleepMs > 1000) sleepMs = 1000;
			}
		  }
		  //wait for poolXceiverServer to terminate 
		  try {
			this.poolXceiverServer.join();	
		  }  catch (InterruptedException ie) {}
		}
	}
	
	//check /image/fsimage
	public boolean isConversionNeeded(PoolFile from) 
			throws IOException {
		File oldF = from.getLocalFile();
		if (!oldF.exists()) { 
			return false;
		}
		RandomAccessFile oldFile = new RandomAccessFile(oldF, "rws");
		try {
			oldFile.seek(0);
			int oldVersion = oldFile.readInt();
			if (oldVersion < StorageClover.LAST_PRE_UPGRADE_LAYOUT_VERSION)
				return false;
		} finally {
			oldFile.close();
		}
		return true;
	}

	//read checkpoint time
	public long readCheckpointTime(PoolFile file) 
			throws IOException {
		//LOG.info("call rpc to read ckptfile:" + file.getName());
		File ckptTime = file.getLocalFile();
	    	long timeStamp = 0L;
	    	if (ckptTime.exists() && ckptTime.canRead()) {
	      		DataInputStream in = new DataInputStream(new FileInputStream(ckptTime));
	      		try {
	        		timeStamp = in.readLong();
	      		} finally {
	        	in.close();
	      		}
	    	} else {
	    		throw new IOException("ckpttime file not found");
	    	}
		return timeStamp;		
	}


	//others has notify format
	public void notifyFormat(String name) {
		for (NameNodeInfo node : this.storage.getPoolNodes()) {
			if (node.getHostName().equalsIgnoreCase(name)) {
				node.setFormat();
			}
		}
	}

	//others has notify format
	public void notifyReport(String name) {
		for (NameNodeInfo node : this.storage.getPoolNodes()) {
			if (node.getHostName().equalsIgnoreCase(name)) {
				node.setReport();
			}
		}
	}
	
	
	////////////////EditLogFileInputStream interface/////////
	public void newEditLogFileInputStream(PoolFile edit) 
	throws IOException {
	   synchronized (logInputMap) {
		String name = edit.getRootpath() + "/" + edit.getName();
		if (!logInputMap.containsKey(name)) {
			//LOG.info("call rpc to create role wallogfile inputstream: " + name);
			EditLogFileInputStream estream = 
				new EditLogFileInputStream(edit);
			logInputMap.put(name, estream);			
		}	
  	   }
	}


	public String[] loadWalFSEdits(String edit, boolean closeOnExit) 
	throws IOException {
	  synchronized (walTransMap) {
		EditLogInputStream walinput = null;
		int numEdits = 0;
		int logVersion = 0;
		String uuid;
		String status;
		String info;
		
		if (isWalLoad) 
			return trans.toArray(new String[trans.size()]);
		
		//LOG.info("call rpc to create waltransmap from wal:" + edit);
		walinput = logInputMap.get(edit);
		DataInputStream in = walinput.getDataInputStream();
	    	long startTime = System.currentTimeMillis();
	    
	    
	    //read wal log fiel version
	    try {
	    	in.mark(4);
	    	boolean available = true;
	    	try {
	    		logVersion = in.readByte();
	    	} catch (EOFException e) {
	    		available = false;
	    	}
	    	if (available) {
	    		in.reset();
	    		logVersion = in.readInt();
	    		if (logVersion < FSConstants.LAYOUT_VERSION) // future version
	    	          throw new IOException(
	    	        		  "Unexpected version of the file system log file: "
	    	                  + logVersion + ". Current version = " 
	    	                  + FSConstants.LAYOUT_VERSION + ".");
	    	}
	    	//read wal trans record
	    	while(true) {
	    		byte opcode = -1;
	    		try {
	    			in.mark(1);
	    			opcode = in.readByte();
	    			if (opcode == OP_INVALID) {
	    				in.reset();// reset back to end of file if somebody reads it again
	    				break;
	    			}
	    		} catch (EOFException e) {
	    			break; //no more transactions
	    		}
	    		numEdits++;
	    		switch (opcode) {
	    		case OP_WAL:
	    			//wal record: uuid status
	    			uuid = FSImage.readString(in);
	    			status = FSImage.readString(in);
				info = uuid + "/" + status;
	    			//LOG.info("put waltransmap:" + uuid + "-->" + status);
				walTransMap.put(uuid, status);
				trans.add(info);
	    			break;
	    			default: {
	    				throw new IOException("Never seen opcode " + opcode);
	    			}
	    			
	    		}	    		
	    	}
	    	
	    } finally {
	    	if(closeOnExit)
	    		in.close();
	    }	    
	    
	    isWalLoad = true;
	    //LOG.info("WalEdits file " + walinput.getName()  + " of size " + walinput.length() + " edits # " + numEdits 
	     //   + " loaded in " + (System.currentTimeMillis()-startTime)/1000 + " seconds.");
	    return trans.toArray(new String[trans.size()]);		
	  }
	}
		
	public int available(String edit) throws IOException {
		return logInputMap.get(edit).available();		
	}
	
	public int read(String edit) throws IOException {
		return logInputMap.get(edit).read();
	}
	
	public int read(String edit, byte[] b, int off, int len) 
	throws IOException {
		return logInputMap.get(edit).read(b, off, len);
	}
	
	public void closeInput(String edit) throws IOException {
		synchronized (logInputMap) {
			if (logInputMap.containsKey(edit)){
				//LOG.info("call rpc to close input edit: " + edit);
				logInputMap.get(edit).close();
				logInputMap.remove(edit);
			}
		}

	}
	
	public long lengthInput(String edit) throws IOException {
		return logInputMap.get(edit).length();
	}
	
	////////////////EditLogFileOutputStream interface/////////
	public void newEditLogFileOutputStream(PoolFile edit, int size, boolean isrole) 
	throws IOException {
	   synchronized (logOutputMap) {
		String name = edit.getRootpath() + "/" + edit.getName();
		if (!logOutputMap.containsKey(name)) {
			EditLogFileOutputStream estream = null;
			if (isrole) {
				//LOG.info("call rpc to create role wallogfile outputstream: " + name);
				//set constant node for wal file
				int replicanum = 0;
				if (StorageClover.getPoolNodes().size() <2 ) {
					replicanum = 1;
				}else if (StorageClover.getPoolNodes().size() <3 ) {
					replicanum = 2;
				} else {
					replicanum = 3;
				} 
				for (int i=0; i < replicanum; i++) {
					NameNodeInfo node = StorageClover.getPoolNodes().get(i);
					edit.getNodeInfos().add(node);
				}

				estream = new EditLogFileOutputStream(edit, size);
			} else {
				//LOG.info("call rpc to create editlogfile outputstream: " + name);
				estream = new EditLogFileOutputStream(new File(name), size);
			}
			logOutputMap.put(name, estream);			
		}
	    }	
	}
	
	public void write(String edit, int b) throws IOException {
		//LOG.info("call rpc to write byte to output edit:" + edit);
		logOutputMap.get(edit).write(b);		
	}
	
	public void write(String edit, Writable[] writables)
	throws IOException {
		
		//LOG.info("call rpc to write writable[] to output edit: " + edit);
		logOutputMap.get(edit).write(writables);
	}
	
	public void write(String edit, Writable w)
	throws IOException {
		
		//LOG.info("call rpc to write writable to output edit: " + edit);
		logOutputMap.get(edit).write(w);
	}

	public String writeTrans(String edit, String uuid, 
	    String status, byte op, Writable[] writables) throws IOException {
		//LOG.info("call rpc to write writable to wal output edit: " + edit);
		synchronized (walTransMap) {
			if (walTransMap.containsKey(uuid)) {
				//LOG.info("logWalEdit: trans:" + uuid + "has exit");
				return walTransMap.get(uuid);
			}
			
			logOutputMap.get(edit).write(op, writables);	
			//sync wal to disk simultaneous
			logOutputMap.get(edit).setReadyToFlush();
			logOutputMap.get(edit).flush();
			
			walTransMap.put(uuid, status);
			return status;
		}
	}

	public void create(String edit) throws IOException {
		////LOG.info("call rpc to create output edit: " + edit);
		logOutputMap.get(edit).create();
	}
	
	public void closeOutput(String edit) throws IOException {
		////LOG.info("call rpc to close output edit: " + edit);
		logOutputMap.get(edit).close();	
		logOutputMap.remove(edit);
	}
	
	public void setReadyToFlush(String edit) throws IOException {
		////LOG.info("call rpc to set readytoflush to output edit: " + edit);
		logOutputMap.get(edit).setReadyToFlush();
	}

	public void flush(String edit) throws IOException {
		////LOG.info("call rpc to flush edit: " + edit);
		logOutputMap.get(edit).flush();
	}

	public void createWal(String edit) throws IOException {
		synchronized (logOutputMap) {
			if (logOutputMap.containsKey(edit)) {
				////LOG.info("call rpc to create wal edit: " + edit);
				logOutputMap.get(edit).create();
				logOutputMap.get(edit).close();
				logOutputMap.remove(edit);
			}
		}
	}

	public void closeWal(String edit) throws IOException {
		synchronized (logOutputMap) {
			if (logOutputMap.containsKey(edit)) {
				////LOG.info("call rpc to close wal edit: " + edit);
				logOutputMap.get(edit).setReadyToFlush();
				logOutputMap.get(edit).flush();
				logOutputMap.get(edit).close();
				logOutputMap.remove(edit);
			}
		}
	}
	
}
