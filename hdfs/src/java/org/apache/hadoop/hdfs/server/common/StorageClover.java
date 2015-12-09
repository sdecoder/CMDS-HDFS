/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.common;

import java.io.*;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.*;
import java.net.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.PoolFile.NameNodeInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.PoolFile.*;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

@InterfaceAudience.Private
public abstract class StorageClover extends StorageInfo{
	public static final Log LOG = LogFactory.getLog(StorageClover.class.getName());
	
	//Constants
	// last layout version that did not suppot upgrades
	protected static final int LAST_PRE_UPGRADE_LAYOUT_VERSION = -3;
	  
	// this corresponds to Hadoop-0.14.
	public static final int LAST_UPGRADABLE_LAYOUT_VERSION = -7;
	protected static final String LAST_UPGRADABLE_HADOOP_VERSION = "Hadoop-0.14";

	/* this should be removed when LAST_UPGRADABLE_LV goes beyond -13.
	 * any upgrade code that uses this constant should also be removed. */
	public static final int PRE_GENERATIONSTAMP_LAYOUT_VERSION = -13;
	  
	// last layout version that did not support persistent rbw replicas
	public static final int PRE_RBW_LAYOUT_VERSION = -19;
	 
	private   static final String STORAGE_FILE_LOCK     = "in_use.lock";
	protected static final String STORAGE_FILE_VERSION  = "VERSION";
	public static final String STORAGE_DIR_CURRENT   = "current";
	private   static final String STORAGE_DIR_PREVIOUS  = "previous";
	private   static final String STORAGE_TMP_REMOVED   = "removed.tmp";
	private   static final String STORAGE_TMP_PREVIOUS  = "previous.tmp";
	private   static final String STORAGE_TMP_FINALIZED = "finalized.tmp";
	private   static final String STORAGE_TMP_LAST_CKPT = "lastcheckpoint.tmp";
	//private   static final String STORAGE_PREVIOUS_CKPT = "previous.checkpoint";
	//replace with checkpointtime1, checkpointtime2
	private static final String STORAGE_PREVIOUS_CKPT = "checkpoints";
	
	public enum StorageState {
	    NON_EXISTENT,
	    NOT_FORMATTED,
	    COMPLETE_UPGRADE,
	    RECOVER_UPGRADE,
	    COMPLETE_FINALIZE,
	    COMPLETE_ROLLBACK,
	    RECOVER_ROLLBACK,
	    COMPLETE_CHECKPOINT,
	    RECOVER_CHECKPOINT,
	    NORMAL;		
	}	
	
	//An interface to denote storage directory type
	public interface StorageDirType {
		public StorageDirType getStorageDirType();
		public boolean isOfType(StorageDirType type);
	}
	
	//use "checkpint1, checkpint2,...,checkpintn"
	private	static final String STORAGE_CKPT = "checkpoint";
	protected static final int POOLFILE_REPLICA = 3;
		
	private Configuration conf = new Configuration();
	
	/** storage pool server, initialize in construct fun*/
	PoolServer server;
		
	private NodeType storageType; //Type of the node
	//store ${dfs.namenode.name.dir} list, indicate logical view
	protected List<StorageDirectory> storageDirs = new ArrayList<StorageDirectory>();
	//store storage directory view: 
	//${dfs.namenode.name.dir}/node1/  	
	//${dfs.namenode.name.dir}/node2/
	//dirMap: @param String hostname
	//        @param PoolDirectory directory list
	protected static Map<String, Map<String,PoolDirectory>> dirMap = 
					new HashMap<String, Map<String, PoolDirectory>>();  
	
	//store namenode ring information
	protected static List<NameNodeInfo> poolNodes; 
	protected NameNodeInfo localnode;
	
	protected Map<String, Map<String,PoolDirectory>> getDirMap(){
		return dirMap;	
	}
		
	//remain unchanged
	private class DirIterator implements Iterator<StorageDirectory> {
		StorageDirType dirType;
		int prevIndex; //for remove()
		int nextIndex; //for next()
		
		DirIterator(StorageDirType dirType) {
			this.dirType = dirType;
			this.nextIndex = 0;
			this.prevIndex = 0;						
		}
		
		public boolean hasNext() {
			if(storageDirs.isEmpty() || nextIndex >= storageDirs.size())
				return false;
			if(dirType != null) {
				while (nextIndex < storageDirs.size()) {
					if(getStorageDir(nextIndex).getStorageDirType().isOfType(dirType))
						break;
					nextIndex++;
				}
				if(nextIndex >= storageDirs.size())
					return false;
			}
			return true;
		}
		
		public StorageDirectory next() {
			StorageDirectory sd = getStorageDir(nextIndex);
			prevIndex = nextIndex;
			nextIndex++;
			if(dirType != null) {
				while (nextIndex < storageDirs.size()) {
					if (getStorageDir(nextIndex).getStorageDirType().isOfType(dirType))
						break;
					nextIndex++;
				}
			} 
			return sd;
		}
		
		public void remove() {
			nextIndex = prevIndex; //restore previous state
			storageDirs.remove(prevIndex);
			hasNext();
		}
	}
	
	/**
	 * Return default iterator
	 */
	public Iterator<StorageDirectory> dirIterator() {
		return dirIterator(null);
	}
	
	/**
	 * Return iterator based on Storage Directory Type 
	 */
	public Iterator<StorageDirectory> dirIterator(StorageDirType dirType) {
		return new DirIterator(dirType);
	}
		
	/**
	 * Generate iterator storage list
	 */
	public String listStorageDirectories() {
		StringBuilder buf = new StringBuilder();
		for(StorageDirectory sd : storageDirs) {
			buf.append(sd.getRoot() + "(" + sd.getStorageDirType() + ");");
		}
		return buf.toString();
	}
		
    /**
     * One of the pool storage directories, operate dirMap 
     */
	public class StorageDirectory{
		//directory list, the root is ${dfs.namenode.name.dir}
		//operate the logical poolDirs view
		File 			root;	//local root directory 
		FileLock 		lock;	//storage lock
		StorageDirType 	dirType;//storage dir type
		
		/**
		 * pooldierctories is constructed as below:
		 * ${dfs.namenode.name.dir}/node1/current/*
		 * ${dfs.namenode.name.dir}/node1/image/*
		 * ${dfs.namenode.name.dir}/node1/checkpoint1/nodeXX/current/*
		 * ${dfs.namenode.name.dir}/node2/current/*
		 */
		
		//set root dir, not changed 
		public StorageDirectory(File dir){
			//default dirType is null
			this(dir, null);			
		}
		
		/**
		 * set root dir and dirtype
		 * for normal operation, root is ${root};
		 * for importcheckpoint directory, configuration root
		 * is like ${root}/checkpointXXX, need to change it 
		 * like ${root}/node1/checkpointXXX;
		 * full path in storage is like 
		 * ${root}/node1/checkpointXXX/node1/current/fsimage
		 */
		public StorageDirectory(File dir, StorageDirType dirType){
			
			//check if importcheckpoint directory
			String name = dir.toString();
			int lastSlash = name.lastIndexOf('/');
			name = name.substring(lastSlash+1);
			if (name.indexOf("checkpoint") >=0 ) {
				String pathname = dir.toString().substring(0, lastSlash)
							+ "/" + localnode.getHostName() + "/" + name;
				dir = new File(pathname);
			}
			//set root value
			this.root = dir;
			this.lock = null;
			this.dirType = dirType;
		}		
		
		/**
		 * Get root directory of this storage
		 */
		public File getRoot() {
			return root;
		}
		
		/**
		 * Get storage directory type
		 */
		public StorageDirType getStorageDirType() {
			return dirType;
		}
		
		/**
		 * Read version file.
		 * 
		 * @throws IOException if file cannot be read or contains inconsistent data
		 */
		public void read() throws IOException {
			read(getVersionFile());			
		}
		
		/**
		 * Read a file from storage pool
		 * need modify getVersionFile() and etc
		 * @param  from= new PoolFile()
		 */
		public void read(File from) throws IOException {
			//first read from local
			//fsimage override getFields
			NameNode.client.readVersion((PoolFile)from, localnode, this);			
		}
				
		/**
		 * Write version file
		 * 
		 * @throws IOException
		 */
		public void write() throws IOException {
			//need modify ??? fixme
			corruptPreUpgradeStorage(root);
			//fsimage override setFiels
			write(getVersionFile()); 			
		}
		
		/**
		 * Write a file to storage pool, not use pipeline
		 * need modify getVersionFile and etc
		 * @param to = new PoolFile()
		 */		
		public void write(File to) throws IOException {
			String[] values = new String[6];
			values[0] = String.valueOf(layoutVersion);
			values[1] = storageType.toString();
			values[2] = String.valueOf(namespaceID);
			values[3] = String.valueOf(cTime);
			String[] array = getOtherFields();
			if (array != null) {
				values[4] = array[0];
				values[5] = array[1];
			} else {
				values[4] = null;
				values[5] = null;
			}
			long time = getCheckpointTime();
			NameNode.client.writeVersion((PoolFile)to, values, time);	
		}
				
		/*
		 * Clear and re-create storage directory
		 * Modify dirMap in memory firstly, then 
		 * Clear local and all other remote nodes by RPC
		 * @throw IOException
		 */
		public void clearDirectory() throws IOException {
			PoolFile curDir = (PoolFile)this.getCurrentDir();
			//try clear dir in all namenodes
			//for test
			if(curDir.getLocalFile().exists()){
				LOG.info("first full delete dir : " + curDir.getRootpath() + "/" + 
					curDir.getName());
				NameNode.client.fullDelete(curDir);
			}
			
			LOG.info("start mkdirs");
			//mkdir with rpc to all nodes
			if(!curDir.mkdirs())
				throw new IOException("Cannot create directory" + curDir);
		}
		
		/**
		 * get /hostname/current dir
		 * @return PoolFile, not with nodeinfo
		 */
		public File getCurrentDir() {
			StringBuilder buf = new StringBuilder();
			buf.append(localnode.getHostName()+ "/" + STORAGE_DIR_CURRENT + "/");			
			return new PoolFile(root, buf.toString(), true);
		}

				/**
		 * get /walhostname/current dir
		 * select poolnodes[0] as wal role host
		 * @return PoolFile, not with nodeinfo
		 */
		public File getWalCurrentDir() {
			StringBuilder buf = new StringBuilder();
			buf.append(getPoolNodes().get(0).getHostName()+ "/" + STORAGE_DIR_CURRENT + "/");			
			return new PoolFile(root, buf.toString(), true);
		}
					
		/**
		 * VERSION File contains
		 * <ol>
		 * <li>node type</li>
		 * <li>layout version</li>
		 * <li>namespaceID</li>
		 * <li>fs state creation time</li>
		 * <li>other fields specific for this node type</li>
		 * </ol> 
		 * @return logical VERSION PoolFile object
		 */		
		public File getVersionFile() {
			StringBuilder buf = new StringBuilder();
			buf.append(localnode.getHostName() + "/" + 
					STORAGE_DIR_CURRENT + "/" + STORAGE_FILE_VERSION);
			return new PoolFile(root, buf.toString());
		}
		
		/**
		 * get the logical previous version PoolFile
		 */
		public File getPreviousVersionFile() {
			StringBuilder buf = new StringBuilder();
			buf.append(localnode.getHostName() + "/" +
					STORAGE_DIR_PREVIOUS + "/" + STORAGE_FILE_VERSION);
			return new PoolFile(root, buf.toString());
		}
		
		/**
		 * @return the previous directory path PoolFile
		 */
		public File getPreviousDir() {
			StringBuilder buf = new StringBuilder();
			buf.append(localnode.getHostName() + "/" + STORAGE_DIR_PREVIOUS + "/");	
			return new PoolFile(root, buf.toString(), true);
		}
		
		/**
		 * @return the previous.tmp directory path PoolFile
		 */
		public File getPreviousTmp() {
			StringBuilder buf = new StringBuilder();
			buf.append(localnode.getHostName()+ "/" + STORAGE_TMP_PREVIOUS + "/");			
			return new PoolFile(root, buf.toString(), true);
		}
		
		/**
		 * @return the removed.tmp directory path PoolFile
		 */
		public File getRemovedTmp() {	
			StringBuilder buf = new StringBuilder();
			buf.append(localnode.getHostName()+ "/" + STORAGE_TMP_REMOVED + "/");			
			return new PoolFile(root, buf.toString(), true);
		}
		
		/**
		 * @return finalized.tmp directory path PoolFile
		 */
		public File getFinalizedTmp() {
			StringBuilder buf = new StringBuilder();
			buf.append(localnode.getHostName()+ "/" + STORAGE_TMP_FINALIZED + "/");			
			return new PoolFile(root, buf.toString(), true);
		}
		
		/**
		 * @return lastcheckpoint.tmp directory path PoolFile
		 */
		public File getLastCheckpointTmp() {
			StringBuilder buf = new StringBuilder();
			buf.append(localnode.getHostName()+ "/" + STORAGE_TMP_LAST_CKPT + "/");			
			return new PoolFile(root, buf.toString(), true);
		}
		
		/**
		 * @return previous.checkpoint directory path PoolFile
		 */
		public File getPreviousCheckpoint() {
			StringBuilder buf = new StringBuilder();
			buf.append(localnode.getHostName()+ "/" + STORAGE_PREVIOUS_CKPT
					+ System.currentTimeMillis() + "/" + localnode.getHostName() + "/current/");			
			return new PoolFile(root, buf.toString(), true);		
		}
		
		/**
		 * check consistency of the storage directory with poolDirs
		 * @param startOpt a startup option
		 * 
		 * @return state (@link StorageState) of the storage directory
		 * @throws InconsistentFSStateException if directory state is not 
		 * consistent and cannot be recovered.
		 * @throws IOException
		 */
		public StorageState analyzeStorage(StartupOption startOpt) throws IOException {
			assert root != null : "root is null";
			String rootPath = root.getCanonicalPath();
			LOG.info("analyze storage, root:" + rootPath);
			try {//check that storage root exists
				if (!root.exists()) {
					//storage directory does not exist
					if (startOpt != StartupOption.FORMAT) {
						LOG.info("Storage directory " + rootPath + "does not exist.");
						return StorageState.NON_EXISTENT;
					}	
					LOG.info(rootPath + "does not exist. Creating ...");
					if (!root.mkdirs())
						throw new IOException("Cannot create directory " + rootPath);
				}
				// or is inaccessible
				if (!root.isDirectory()) {
					LOG.info(rootPath + "is not a directory.");
					return StorageState.NON_EXISTENT;
				}
				if (!root.canWrite()) {
					LOG.info("Cannot access storage directory " + rootPath);
					return StorageState.NON_EXISTENT;
				}
			} catch(SecurityException ex) {
				LOG.info("Cannot access storage directory " + rootPath, ex);
				return StorageState.NON_EXISTENT;				
			}
			this.lock(); // lock storage if it exists
			if (startOpt == HdfsConstants.StartupOption.FORMAT)
				return StorageState.NOT_FORMATTED;
			if (startOpt != HdfsConstants.StartupOption.IMPORT) {
				checkConversionNeeded(this);
			}
			
			//check whether current directory is valid
			File versionFile = getVersionFile();
			boolean hasCurrent = versionFile.exists();//poolfile call
			
			//check which directories exist, poolfile call
			boolean hasPrevious = getPreviousDir().exists();
			boolean hasPreviousTmp = getPreviousTmp().exists();
			boolean hasRemovedTmp = getRemovedTmp().exists();
			boolean hasFinalizedTmp = getFinalizedTmp().exists();
			boolean hasCheckpointTmp = getLastCheckpointTmp().exists();
			
			if (!(hasPreviousTmp || hasRemovedTmp
					|| hasFinalizedTmp || hasCheckpointTmp)) {
				// no temp dirs - no recovery
			    if (hasCurrent){
			        LOG.info("has current dir, return:" + StorageState.NORMAL);	
				return StorageState.NORMAL;
			    }
			    if (hasPrevious)
			    	throw new InconsistentFSStateException(root,
			        			"version file in current directory is missing.");
			    return StorageState.NOT_FORMATTED;
			}
			
			if ((hasPreviousTmp?1:0) + (hasRemovedTmp?1:0)
					+ (hasFinalizedTmp?1:0) + (hasCheckpointTmp?1:0) > 1)
			    // more than one temp dirs
				throw new InconsistentFSStateException(root,
			              "too many temporary directories.");
			
		    // # of temp dirs == 1 should either recover or complete a transition
		    if (hasCheckpointTmp) {
		    	return hasCurrent ? StorageState.COMPLETE_CHECKPOINT
		                          : StorageState.RECOVER_CHECKPOINT;
		    }
		    
		    if (hasFinalizedTmp) {
		        if (hasPrevious)
		        	throw new InconsistentFSStateException(root,
		                                                   STORAGE_DIR_PREVIOUS + " and " + STORAGE_TMP_FINALIZED
		                                                   + "cannot exist together.");
		        return StorageState.COMPLETE_FINALIZE;
		    }

		    if (hasPreviousTmp) {
		        if (hasPrevious)
		        	throw new InconsistentFSStateException(root,
		                                                   STORAGE_DIR_PREVIOUS + " and " + STORAGE_TMP_PREVIOUS
		                                                   + " cannot exist together.");
		        if (hasCurrent)
		        	return StorageState.COMPLETE_UPGRADE;
		        return StorageState.RECOVER_UPGRADE;
		    } 
		    
		    assert hasRemovedTmp : "hasRemovedTmp must be true";
		    if (!(hasCurrent ^ hasPrevious))
		    	throw new InconsistentFSStateException(root,
		                                               "one and only one directory " + STORAGE_DIR_CURRENT 
		                                               + " or " + STORAGE_DIR_PREVIOUS 
		                                               + " must be present when " + STORAGE_TMP_REMOVED
		                                               + " exists.");
		    if (hasCurrent)
		    	return StorageState.COMPLETE_ROLLBACK;
		    return StorageState.RECOVER_ROLLBACK;
		}
		
		/**
		 * Complete or recover storage from previously failed transition
		 * @throws IOException
		 */
		public void doRecover(StorageState curState) throws IOException {
			File curDir = getCurrentDir(); //poolfile call
			String rootPath = root.getCanonicalPath();
			switch(curState) {
				case COMPLETE_UPGRADE: // mv previous.tmp -> previous
					LOG.info("Completing previous upgrade for storage directory " 
							+ rootPath + ".");
					rename(getPreviousTmp(), getPreviousDir());
					return;
				case RECOVER_UPGRADE: // mv previous.tmp -> current
					LOG.info("Recovering storage directory " + rootPath
				             + " from previous upgrade.");
					if (curDir.exists()) //poolfile call
						deleteDir(curDir);
					rename(getPreviousTmp(), curDir);
					return;
				case COMPLETE_ROLLBACK: // rm removed.tmp
			        LOG.info("Completing previous rollback for storage directory "
			                 + rootPath + ".");
			        deleteDir(getRemovedTmp());
			        return;
				case RECOVER_ROLLBACK: //mv removed.tmp -> current
					LOG.info("Recovering storage directory " + rootPath
			                 + " from previous rollback.");
			        rename(getRemovedTmp(), curDir);
			        return;
				case COMPLETE_FINALIZE: //rm finalized.tmp
			        LOG.info("Completing previous finalize for storage directory "
			                 + rootPath + ".");
			        deleteDir(getFinalizedTmp());
			        return;
				case COMPLETE_CHECKPOINT: //mv lastcheckpoint.tmp -> checkpointXXX
			        LOG.info("Completing previous checkpoint for storage directory " 
			                 + rootPath + ".");
			        File prevCkptDir = getPreviousCheckpoint();
			        if (prevCkptDir.exists()) //poolfile call
			        	deleteDir(prevCkptDir);
			        rename(getLastCheckpointTmp(), prevCkptDir);
			        return;
				case RECOVER_CHECKPOINT: //mv lastcheckpoint.tmp -> current
					LOG.info("Recovering storage directory " + rootPath
			                 + " from failed checkpoint.");
					if (curDir.exists()) //poolfile call
						deleteDir(curDir);
					rename(getLastCheckpointTmp(), curDir);
					return;
				default:
					throw new IOException("Unexcepted FS state:" + curState);			        
			}	
		}
				
		/*
		 * Lock storage to provide exclusive access
		 * only operate in local node
		 */
		public void lock() throws IOException {
			this.lock = tryLock();
			if (lock == null) {
				String msg = "Cannot lock storage" + this.root
				+ "/" + localnode.getHostName() + "/. The directory is already locked.";
				LOG.info(msg);
				throw new IOException(msg);
			}
		}
		
		/*
		 * Attempts to acquire an exclusive lock on the storge
		 * only operate in local node
		 */
		FileLock tryLock() throws IOException {
			File lockF = new File(root + "/" +localnode.getHostName() + "/", STORAGE_FILE_LOCK );
			lockF.deleteOnExit();
			RandomAccessFile file = new RandomAccessFile(lockF, "rws");
			FileLock res = null;
			try {
				LOG.info("trylock file :" + lockF.getPath());
				res = file.getChannel().tryLock();
			} catch(OverlappingFileLockException oe) {
				file.close();
				return null;
			} catch (IOException e) {
				LOG.error("Cannot create lock on " + lockF, e);
				file.close();
				throw e;
			}
			return res;
		}
		
		/*
		 * Unlock storage, only operate in local node
		 */
		public void unlock() throws IOException {
			if (this.lock == null)
				return;
			this.lock.release();
			this.lock.channel().close();
			this.lock = null;
			LOG.info("after unlock");
		}		
	}
	
	/**
	 * Create empty storage info of the specified type
	 * and get local node info
	 */
	protected StorageClover(NodeType type) {
		super();
		fillLocalNode();
		fillPoolNodes();
		this.storageType = type;
		/** storage pool client */
		LOG.info("create NameNode client");
		NameNode.client = new PoolClient(this);
		try {
			//create rpc and socket server
			server = new PoolServer(this);
		} catch (IOException ie){
			LOG.info("create poolserver error: " + ie);
		};
	}
	
	protected StorageClover(NodeType type, int nsID, long cT) {
		super(FSConstants.LAYOUT_VERSION, nsID, cT);
		fillLocalNode();
		fillPoolNodes();
		this.storageType = type;
		NameNode.client = new PoolClient(this);
		try {
			//create rpc and socket server
			server = new PoolServer(this);
		} catch (IOException ie){
			LOG.info("create poolserver error: " + ie);
		};
	}
	
	public static void DisableLogging() {
		LOG.info("disable FSNamesystem log info\n");
		Logger.getLogger(FSNamesystem.class).setLevel(Level.ERROR);
		LOG.info("disable Namenode log info\n");
		Logger.getLogger(NameNode.class).setLevel(Level.ERROR);
		LOG.info("disable datanode log info\n");
		Logger.getLogger(DataNode.class).setLevel(Level.ERROR);	
	}
	
	protected StorageClover(NodeType type, StorageInfo storageInfo) {
		super(storageInfo);
		fillLocalNode();
		fillPoolNodes();
		this.storageType = type;
		NameNode.client = new PoolClient(this);
		try {
			//create rpc and socket server
			server = new PoolServer(this);
		} catch (IOException ie){
			LOG.info("create poolserver error in StroageClover()");
		};
	}
	
	// read local node info from configuration
	protected void fillLocalNode() {
		
		PoolFile pf = new PoolFile();
		String name = ""; ///hostname:portNumber
		String hostname = ""; 
		int rpcport = 9000; //default
		int socketport = 9001; //default
		int colon;
		//get local socket addr, name
		try{
		   InetAddress ia = InetAddress.getLocalHost();
		   hostname = ia.getHostName();
		   LOG.info("localhost name: " + hostname);
		} catch (UnknownHostException e) {
		   LOG.error("can't get local hostname");	   
		}
		
		name = conf.get("dfs.poolnode.address", "0.0.0.0:60011");
		colon = name.indexOf(":");
		if (colon > 0) {
			socketport = Integer.parseInt(name.substring(colon+1));
		}
		name = conf.get("dfs.poolnode.ipc.address", "0.0.0.0:60021");
		colon = name.indexOf(":");
		if (colon > 0)
			rpcport = Integer.parseInt(name.substring(colon+1));
		localnode = pf.new NameNodeInfo(hostname, rpcport, socketport);		
	}
	
	public NameNodeInfo getLocalNode() {
		return localnode;
	}
	
	//fill poolNodes information from configuration
	//FIXME
	public void fillPoolNodes() {
		//FIXME read from configure file
		//set poolNodes 
		try {
			poolNodes = new ArrayList<NameNodeInfo>();
			NameNodeInfo node = null;
			String hostname;
			Collection<URI> dnnNames = NameNode.getDnnURI(conf);
			ArrayList<URI> names = (ArrayList<URI>) dnnNames;
			final String dfs_poolnode_string = conf.get(DFSConfigKeys.DFS_POOLNODE_ADDRESS);
			final String dfs_poolnode_ipc_string = conf.get(DFSConfigKeys.DFS_POOLNODE_IPC_ADDRESS);
			final int dfs_poolnode_address = Integer.valueOf(dfs_poolnode_string.split(":")[1]);
			final int dfs_poolnode_ipc_address =  Integer.valueOf(dfs_poolnode_ipc_string.split(":")[1]);

			for (int i = 0; i < names.size(); i++) {
				PoolFile tmp = new PoolFile();
				hostname = names.get(i).getHost();
				node = tmp.new NameNodeInfo(hostname, 
						dfs_poolnode_ipc_address, 
						dfs_poolnode_address);
				LOG.error("poolnode add:" + hostname 
						+ " ipc-port: " + dfs_poolnode_ipc_address
						+ " poolnode-addr: " + dfs_poolnode_address);
				
				poolNodes.add(node);		
			}
		} catch (Exception e) {
			LOG.error(StringUtils.stringifyException(e));
		}
		
	}
	
	public static List<NameNodeInfo> getPoolNodes() {
		return poolNodes;
	}
	
	//unchanged
	public int getNumStorageDirs() {
		return storageDirs.size();
	}
	
	//unchanged
	public StorageDirectory getStorageDir(int idx) {
		return storageDirs.get(idx);
	}
	
	//unchanged
	protected void addStorageDir(StorageDirectory sd) {
		storageDirs.add(sd);
	}
		
	
	//image/fsimage is used to prevent hadoop from starting before version 0.13  
	//???remove abstract modify, replace implement in FSImage
	//public abstract boolean isConversionNeeded(StorageDirectory sd) throws IOException;
	public boolean isConversionNeeded(StorageDirectory sd) 
		throws IOException {
		return NameNode.client.isConversionNeeded(sd, localnode);		
	}
	
	
	/*
	 * Coversion is no longer supported. So this should throw exception if
	 * conversion is needed.
	 */
	private void checkConversionNeeded(StorageDirectory sd) throws IOException {
		if (isConversionNeeded(sd)){
			//throw an exception
			checkVersionUpgradable(0);
		}
	}
		
	/**
	  * Checks if the upgrade from the given old version is supported. If
	  * no upgrade is supported, it throws IncorrectVersionException.
	  * 
	  * @param oldVersion
	  */
	protected static void checkVersionUpgradable(int oldVersion) 
								throws IOException {
		if (oldVersion > LAST_UPGRADABLE_LAYOUT_VERSION) {
			String msg = "*********** Upgrade is not supported from this older" +
	                   " version of storage to the current version." + 
	                   " Please upgrade to " + LAST_UPGRADABLE_HADOOP_VERSION +
	                   " or a later version and then upgrade to current" +
	                   " version. Old layout version is " + 
	                   (oldVersion == 0 ? "'too old'" : (""+oldVersion)) +
	                   " and latest layout version this software version can" +
	                   " upgrade from is " + LAST_UPGRADABLE_LAYOUT_VERSION +
	                   ". ************";
			LOG.error(msg);
			throw new IOException(msg); 
	    }
	}	
	
	/**
	 * Get common storage fields
	 * 
	 * @param props
	 * @throws IOException
	 */
	protected void getFields(Properties props, 
							StorageDirectory sd
							) throws IOException {
		String sv, st, sid, sct;
		sv = props.getProperty("layoutVersion");
		st = props.getProperty("storageType");
		sid = props.getProperty("namespaceID");
		sct = props.getProperty("cTime");
		if (sv == null || st == null ||sid == null ||sct ==null )
			 throw new InconsistentFSStateException(sd.root,
                     "file " + STORAGE_FILE_VERSION + " is invalid.");
		int rv = Integer.parseInt(sv);
		NodeType rt = NodeType.valueOf(st);
		int rid = Integer.parseInt(sid);
		long rct = Long.parseLong(sct);
		if (!storageType.equals(rt) ||
		        !((namespaceID == 0) || (rid == 0) || namespaceID == rid))
		      throw new InconsistentFSStateException(sd.root,
		                                             "is incompatible with others.");
		if (rv < FSConstants.LAYOUT_VERSION) // future version
		      throw new IncorrectVersionException(rv, "storage directory " 
		                                          + sd.root.getCanonicalPath());
		
		//set storageinfo value
		layoutVersion = rv;
		storageType = rt;
		namespaceID = rid;
		cTime = rct;
	}
	
	/**
	 * Set common storage fields  
	 * 
	 * @param props
	 * @throws IOException
	 */
	protected void setFields(Properties props, 
		StorageDirectory sd
		) throws IOException {
		LOG.info("enter setfields");
		props.setProperty("layoutVersion", String.valueOf(layoutVersion));
		props.setProperty("storageType", storageType.toString());
		props.setProperty("namespaceID", String.valueOf(namespaceID));
		props.setProperty("cTime", String.valueOf(cTime));		
	}

	// get other properties, for fsimage override
	protected String[] getOtherFields() {
		return null;
	}

	// get checkpoint time, for fsimage override
	protected long getCheckpointTime() {
		return System.currentTimeMillis();	
	}

	
	/**
	 * rename in storage pool, always with directories
	 * consider the failure conditions
	 * @param from, to is PoolFile 
	 */
	
	public void rename(File from, File to) throws IOException {
		//broadcast all namenodes through rpc
		NameNode.client.rename((PoolFile)from, (PoolFile)to, localnode);
	}

	/**
	 * delete the directory
	 */
	protected void deleteDir(File dir) throws IOException {
	  //broadcast all namenodes through rpc	
		NameNode.client.fullDelete((PoolFile)dir);	
	}
	
	/**
	 * Write all data storage files
	 * @throws IOException
	 */
	public void writeAll() throws IOException {
		this.layoutVersion = FSConstants.LAYOUT_VERSION;
		for (Iterator<StorageDirectory> it = storageDirs.iterator(); it.hasNext();) {
			it.next().write();
		} 				
	}
	
	/**
	 * Unlock all storage directories
	 * @throws IOException
	 */
	public void unlockAll() throws IOException {
		for (Iterator<StorageDirectory> it = storageDirs.iterator(); it.hasNext();) {
			it.next().unlock();
		}
	}
	
	/**
	 * Check file locking support, unchanged
	 * 
	 */
	public boolean isLockSupported(int idx) throws IOException {
	    StorageDirectory sd = storageDirs.get(idx);
	    FileLock firstLock = null;
	    FileLock secondLock = null;
	    try {
	      firstLock = sd.lock;
	      if(firstLock == null) {
	        firstLock = sd.tryLock();
	        if(firstLock == null)
	          return true;
	      }
	      secondLock = sd.tryLock();
	      if(secondLock == null)
	        return true;
	    } finally {
	      if(firstLock != null && firstLock != sd.lock) {
	        firstLock.release();
	        firstLock.channel().close();
	      }
	      if(secondLock != null) {
	        secondLock.release();
	        secondLock.channel().close();
	      }
	    }
	    return false;
	}
	
	public static String getBuildVersion() {
		return VersionInfo.getRevision();
	}
	
	public static String getRegistrationID(StorageInfo storage) {
		return "NS-" + Integer.toString(storage.getNamespaceID())
		      + "-" + Integer.toString(storage.getLayoutVersion())
		      + "-" + Long.toString(storage.getCTime());
	}	
	
	/**
	 *  Pre-upgrade version compatibility
	 *  delete abstract modify 
	 *  protected abstract void corruptPreUpgradeStorage(File rootDir) 
	 */
	//?? need to delete implement in FSImage
	protected void corruptPreUpgradeStorage(File rootDir) 
		throws IOException {
		NameNode.client.corruptPreUpgradeStorage(rootDir, localnode);
	}
	
	//implement in mamenode with rpc
	//protected void writeCorruptedData(RandomAccessFile file) throws IOException{};	

	//shutdown poolserver and poolclient, include all serverces
	public void shutDown() {
		LOG.info("stop poolserver service");
		this.server.shutdown();
		LOG.info("stop poolclient rpc");
		NameNode.client.stop();
	}

}
