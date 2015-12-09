package org.apache.hadoop.hdfs.server.common;

import java.io.IOException;
import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.io.File;
import java.util.ArrayList;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.StorageClover.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.PoolFile.*;


/********************************************************
 * PoolClient can connect to a storage pool node and perform 
 * file/directory operations with RPC.
 *
 ********************************************************/

public class PoolClient implements FSConstants, java.io.Closeable {
	public final Log LOG = LogFactory.getLog(PoolClient.class);
		
	private Configuration conf;
	volatile boolean clientRunning = true;
	Random r= new Random();
	public String clientName;
	StorageClover storage;
	//hashmap to record all rpc proxy
	protected Map<String, PoolProtocol> rpcMap = new HashMap<String, PoolProtocol>();
	
	public int bytesPerChecksum;
	public int packetSize;
	public int socketTimeout;
	public int writeTimeout;
	public static final int STREAM_BUFFER_SIZE_DEFAULT = 1024*1024;
	
	public PoolClient(StorageClover storage){
		this.storage = storage;
		conf = new Configuration();		
		this.clientName = storage.getLocalNode().getHostName();	
		this.bytesPerChecksum = conf.getInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 
	            DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT);
		this.packetSize = conf.getInt(DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, 
	            DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT);
		this.socketTimeout = conf.getInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 
	             HdfsConstants.READ_TIMEOUT);
		this.writeTimeout = conf.getInt("pool.namenode.socket.write.timeout", 
				HdfsConstants.WRITE_TIMEOUT);
		LOG.info("clientName: " + clientName + ", bytesPerChecksum: " + 
			bytesPerChecksum + ", packetSize: " + packetSize + ", readTimeout: " + 
			socketTimeout + ", writeTimeout: " + writeTimeout);
	}
	
	public PoolProtocol start(NameNodeInfo servernode) throws IOException {
		//create RPC client to connect to poolserver
		PoolProtocol poolserver = null;
		InetSocketAddress serverAddr = null; // for create rpc proxy
		poolserver = rpcMap.get(servernode.getHostName());
		if (poolserver == null){	
			serverAddr = NetUtils.createSocketAddr(
						servernode.getRpcAddress());
			poolserver = (PoolProtocol)RPC.getProxy(PoolProtocol.class, 
					PoolProtocol.versionID, serverAddr, UserGroupInformation.getCurrentUser(), conf, 
					NetUtils.getSocketFactory(conf, PoolProtocol.class));
			rpcMap.put(servernode.getHostName(), poolserver);
		}
		return poolserver;
	}
	
	//stop rpc for the poolserver
	public void stop(PoolProtocol poolserver) {
		RPC.stopProxy(poolserver);
	}
	
	//stop all rpc poolserver
	public void stop() {
		synchronized (rpcMap) {
			for (Iterator<PoolProtocol> it = rpcMap.values().iterator();
				it.hasNext();) {
					PoolProtocol thisserver = it.next();
					RPC.stopProxy(thisserver);
			}
		}
	}	
	
	/**
	 * scan local disk and call rpc to report local 
	 * files to all nodes, will add in FSImage.java
	 */
	public int fileReport(NameNodeInfo localNode) throws IOException {
		
		//get storage directory, only report one root directory
		LOG.info("enter file report");
		List<PoolFileInfo> files = new ArrayList<PoolFileInfo>();
		File root = null;
		String filename = ""; //not include ${root}, like /node/current/fsimage	
		int crc = 0;
		File[] filelist = null;
		
		
		
		//get root list
		Iterator<StorageDirectory> it = storage.dirIterator();
		for (; it.hasNext();) {
			//scan local disk within root directories and fill PoolFileInfo[]
			StorageDirectory sd = it.next();
			root = sd.getRoot();
			assert root != null : "storage directory not set";
			LOG.info("sd root is :" + root.getPath());
			
			File[] nodelist = root.listFiles();
			for (int idx = 0; idx < nodelist.length; idx ++) {
				//get directory, like node1, node2,...
				assert nodelist[idx].isDirectory() : "node hostname not directory";
				LOG.info("nodelist[" + idx +"], name: " + nodelist[idx].getPath());
				
				File[] dirlist = nodelist[idx].listFiles();
				//get directory, like current, checkpointXXX,...
				for (int idy =0; idy < dirlist.length; idy ++) {
					//not checkpoint dir, like current
					if(dirlist[idy].toString().lastIndexOf("in_use.lock") >=0) {
						//do not report in_use.lock file
						LOG.info("do not report dir[" + idy + "], name: " + dirlist[idy].getPath());
						continue;						
					}
					if(dirlist[idy].isDirectory() && dirlist[idy].toString().indexOf("checkpoint") < 0) {
						filelist = dirlist[idy].listFiles();
					} else if (dirlist[idy].toString().indexOf("checkpoints") >= 0){
						//first get node name, from dirlist[idy], like:
						//${root}/NetMonitor/checkpoints1319619588545
						int lastSlash = dirlist[idy].toString().lastIndexOf('/');
						String nodename= dirlist[idy].toString().substring(0,lastSlash);
						lastSlash = nodename.lastIndexOf('/');
						nodename = nodename.substring(lastSlash + 1); 

						//checkpoint dir, like checkpointXXX/node/current/
						File dir = new File(dirlist[idy].toString() + "/" + nodename + "/current");
						filelist = dir.listFiles();
					}
					//like:${root}/NetMonitor/checkpoints1319619588545
					LOG.info("get dir[" + idy + "], name: " + dirlist[idy].getPath());
					//get files, like VERSION, fsimage, edit
					for (int idz = 0; idz < filelist.length; idz ++) {
						filename = filelist[idz].getAbsolutePath();
						filename = filename.substring(root.toString().length()+1);
						LOG.info("get file, index: " + idz + ", name: " + filename);
						//FIXME read crc??
						PoolFileInfo file = new PoolFileInfo(filename, crc);
						files.add(file);
					}
				}
			}
		
			//report to all nodes
			for (NameNodeInfo node : storage.getPoolNodes()) {
				start(node).fileReport(localNode.getHostName(), localNode.getRpcPort(),
					localNode.getSocketPort(), files.toArray(new PoolFileInfo[files.size()]));
			}
		}
		return 0;
	}
	
	/**
	 * rename file or directory
	 */
	public void rename(PoolFile from, PoolFile to, NameNodeInfo localNode) 
				throws IOException {
		//for file operations, rpc to relevant namenodes
		if (!from.isDir) {
			for (NameNodeInfo node : from.getNodeInfos()) {
				try {
					start(node).rename(from, to, localNode.getHostName());
				} catch (IOException ie) {
					LOG.info("Problem connecting to server: " + node.getRpcAddress());
					from.removeChild(node);
				}
			}	
		} else {
			//for dir operations, rpc to all nodes
			for (NameNodeInfo node : storage.getPoolNodes()) {
				try {
					start(node).rename(from, to, localNode.getHostName());
				} catch (IOException ie) {
					LOG.info("Problem connecting to server: " + node.getRpcAddress());
				}
			}
		}
	}
	
	/**
	 * create directory
	 */
	public void mkdir(PoolFile dir) {
		for (NameNodeInfo node : storage.getPoolNodes()) {
			try{
				LOG.info("begin call mkdir, hostname: " + node.getHostName() 
				+ ", rpcport: " + node.getRpcPort() + ", socketport: " + 
				node.getSocketPort());
				start(node).mkdir(dir);
			} catch (IOException ie) {
				LOG.info("Problem connecting to server : " + node.getRpcAddress());
			}
		}		
	}
	

	/**
	 * delete the directory
	 */
	public void fullDelete(PoolFile dir) throws IOException {
		//for dir operations, rpc to all nodes
		for (NameNodeInfo node : storage.getPoolNodes()) {
			try{
				LOG.info("call full delete in node: " + node.getHostName());
				start(node).fullDelete(dir);
			} catch (IOException ie) {
				LOG.info("Problem connecting to server: " + node.getRpcAddress());
			}
		}
	}
	
	/**
	 * delete the file
	 */
	public boolean delete(PoolFile file) {
		boolean result = true;
		for (NameNodeInfo node : file.getNodeInfos()) {
			try{
				result = start(node).delete(file);
			} catch (IOException ie) {
			}
		}
		return result;
	}
		
	/**
	 * write version info to ${root}/image/fsimage 
	 */
	public void corruptPreUpgradeStorage(File rootDir, NameNodeInfo localNode) 
				throws IOException {
		
		LOG.info("corruptpreupgradestorage, dir: " + rootDir.getPath() + "/" +
			localNode.getHostName() + "/image");
		PoolFile oldImageDir = new PoolFile(rootDir, 
				 localNode.getHostName() + "/image/", true);
		if (!oldImageDir.exists())
			if (!oldImageDir.mkdir())
				throw new IOException("Cannot create directory" + oldImageDir);
		PoolFile oldImage = new PoolFile(rootDir, localNode.getHostName() + 
				"/image/fsimage", false);
		LOG.info("corruptpreupgradestorage, file: " + rootDir.getPath() + "/" +
			localNode.getHostName() + "/image/fsimage");
		if (!oldImage.exists()) 
			// recreate old image file to let pre-upgrade versions fail
			if (!oldImage.createNewFile())
				throw new IOException("Cannot create file " + oldImage);
		writeCorruptedData(oldImage);
	}
	
	protected void writeCorruptedData(PoolFile file) throws IOException {
		//then call rpc
		for (NameNodeInfo node: file.getNodeInfos()) {
			try{
				long start = System.nanoTime();
				start(node).writeCorruptedData(file);
				long end = System.nanoTime();
				LOG.info("write corrupteddata spend:" + (end-start) + " nanos");
			} catch (IOException ie){
				file.removeChild(node);
				LOG.info("Problem connecting to server: " + node.getRpcAddress());
			}
		}
	}

	//read checkpointtime file
	public long readCheckpointTime(PoolFile file) throws IOException {
		//then call rpc
		long timeStamp = 0L;
		for (NameNodeInfo node: file.getNodeInfos()) {
			try{
				timeStamp = start(node).readCheckpointTime(file);
			} catch (IOException ie){
				file.removeChild(node);
				LOG.info("Problem connecting to server: " + node.getRpcAddress());
			}
			break;
		}
		return timeStamp;
	}
	
	
	public boolean isConversionNeeded(StorageDirectory sd, NameNodeInfo localNode) 
			throws IOException {
		//first read from local
		PoolFile oldImageDir = new PoolFile(sd.getRoot(), 
				localNode.getHostName() + "/image/", true);
		LOG.info("old image dir:" + oldImageDir.getLocalFile().getPath());
		if (!oldImageDir.exists()) {
			if (((PoolFile)sd.getVersionFile()).exists())
				throw new InconsistentFSStateException(sd.getRoot(), 
						oldImageDir + "does not exist.");
			return false;
		}
		PoolFile oldF = new PoolFile(sd.getRoot(), localNode.getHostName() + "/image/fsimage", 
									false);
		RandomAccessFile oldFile = new RandomAccessFile(oldF.getLocalFile(), "rws");
		try {
			oldFile.seek(0);
			int oldVersion = oldFile.readInt();
			if (oldVersion < StorageClover.LAST_PRE_UPGRADE_LAYOUT_VERSION)
				return false;
		} catch (IOException ie) {
			// try read from remote nodes
			boolean result;
			List<NameNodeInfo> nodes = oldF.getNodeInfos();
			if (nodes.isEmpty())
				throw new IOException("no node list for version file" + oldF);	
				for (NameNodeInfo node : nodes) {
					if (node.getHostName() != localNode.getHostName()) {
						result = start(node).isConversionNeeded(oldF);
						return result;
					}					
				}
		} finally {
			oldFile.close();
		}
		return true;
	}
		
	//read version file from storage pool
	public void readVersion(PoolFile from, NameNodeInfo localNode, StorageDirectory sd) 
		throws IOException {
		//first try read from local
		RandomAccessFile file = new RandomAccessFile(from.getLocalFile(), "rws");
		FileInputStream in = null;
		Properties props = null;
		try {
			in = new FileInputStream(file.getFD());
			file.seek(0);
			props = new Properties();
			props.load(in);
			//override by fsimag
			storage.getFields(props,sd);
		} catch (IOException ie) {
			//read error in local, now from remote
			props = null;
			List<NameNodeInfo> nodes  = from.getNodeInfos();
			if (nodes.isEmpty())
				throw new IOException("no node list for version file" + from);
			for (NameNodeInfo node : nodes) {
				if (node.getHostName() != localNode.getHostName()) {
					props = start(node).readVersion(from);
					if (props != null) {
						storage.getFields(props, sd);
						break;
					} else {
						LOG.info("Problem connecting to server: " + node.getRpcAddress());
						from.removeChild(node);
					}
				}
			}
		} finally {
			if (in != null)
				in.close();
			file.close();
		}	
	}

	//notify other nodes me has been formatted
	public void notifyFormat(String name)
		throws IOException {
		//call rpc to write to namenodes in the storage pool
		for (NameNodeInfo node : storage.getPoolNodes()) {
			try{
				LOG.info("begin notify format, hostname: " + node.getHostName() 
				+ ", rpcport: " + node.getRpcPort() + ", socketport: " + 
				node.getSocketPort());
				start(node).notifyFormat(name);
			} catch (IOException ie) {
				LOG.info("Problem connecting to server : " + node.getRpcAddress());
			}
		}		
	}

	//notify other nodes me has reported local file
	public void notifyReport(String name)
		throws IOException {
		//call rpc to write to namenodes in the storage pool
		for (NameNodeInfo node : storage.getPoolNodes()) {
			try{
				LOG.info("begin notify report, hostname: " + node.getHostName() 
				+ ", rpcport: " + node.getRpcPort() + ", socketport: " + 
				node.getSocketPort());
				start(node).notifyReport(name);
			} catch (IOException ie) {
				LOG.info("Problem connecting to server : " + node.getRpcAddress());
			}
		}		
	}
	
	//write version file to storage pool
	public void writeVersion(PoolFile to, String[] values, long ckpTime)
		throws IOException {
		//call rpc to write to namenodes in the storage pool
		List<NameNodeInfo> nodes = to.getNodeInfos();
		for (NameNodeInfo node : nodes) {
			try {
				start(node).writeVersion(to, values, ckpTime);
			} catch (IOException ie) {
				LOG.info("Problem connecting to server: " + node.getRpcAddress());
				to.removeChild(node);
			}
		}
	}
		
	//recover file from remote node
	public void recoverFrom(File file, NameNodeInfo node) 
		throws IOException {
		
	}
	
	//recover file to remote node
	public void recoverTo(File file, NameNodeInfo node)
		throws IOException {
				
	}
	
	//append file for WAL, not use pipeline
	public void appendFile(byte[] b, File file)
		throws IOException {
		
	}
	
	public synchronized void close() throws IOException {
		
	}

	
}
