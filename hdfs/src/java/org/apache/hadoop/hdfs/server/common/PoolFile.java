package org.apache.hadoop.hdfs.server.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.commons.logging.Log;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


/********************************************************
 * PoolFile indicates a logical file or directory 
 * in the storage pool
 *
 ********************************************************/

public class PoolFile extends File implements Writable {
	
	private static final long serialVersionUID = 1L;
	private static final int POOLFILE_REPLICA_NUM = 3;
	public static final Log LOG = LogFactory.getLog(PoolFile.class);
	
	protected String rootpath = ""; //${root}
	protected String name = ""; //not include ${root}
	protected int namenodeNum = 3;
	protected boolean isDir = false;
	//protected PoolClient client = null;
			
	//store the node list the file belongs to
	protected List<NameNodeInfo> nodeInfos = new ArrayList<NameNodeInfo>(namenodeNum);
	
	
	public PoolFile() {
		super("");
	}
	
	//construction for file
	public PoolFile(File root, String name){
		super("");		
		if (root instanceof PoolFile) {
			//construction for such as: getCurrentDir() + "edit"
			PoolFile f = (PoolFile)root;
			this.name = f.getName() + name;
			this.isDir = false;
			this.rootpath = f.getRootpath();
			//LOG.info("call poolfile, rootpath: " + this.rootpath + ", name: " + this.name);
		} else {
			//construction for root + name
			this.name = name;
			this.isDir = false;
			this.rootpath = root.getPath();
			//LOG.info("creae poolfile, rootpath: " + rootpath + ", name: " + name);
		}
		tryFileNodeInfos();
	}
	
	//construction for dirMap
	PoolFile(String name, NameNodeInfo node){
		super("");
		this.name = name;
		this.isDir = false;
		if (node != null)
			this.nodeInfos.add(node);
	}
	
	//construction for directory, like
	//root:/tmp/hadoop-test/dfs/name, dir name: se2/image/  
	//root:/tmp/hadoop-test/dfs/name, name: se2/image/fsimage  
	PoolFile(File root, String name, boolean isDir){
		super("");
		this.name = name;
		this.isDir = isDir;
		this.rootpath = root.getPath();
		//LOG.info("enter poolfile, root: " + root.getPath() + ", name: " + name + ", isdir: " + isDir);
		if (!isDir)
			tryFileNodeInfos();
	}
		
	/**
	 * indicate the logical view in one node directory
	 */
	
	public class PoolDirectory{
		public String dirName = "";
		//store file list belong to the directory
		protected Map<String, PoolFile> poolFiles = 
			new HashMap<String, PoolFile>();
		
		PoolDirectory(String dirName) {
			this.dirName = dirName;
		}
	
		public String getDirName() {
			return dirName;
		}
		
		public void setDirName(String name) {
			this.dirName = name;
		}
		
		public Map<String, PoolFile> getPoolFiles() {
			return poolFiles;
		}
		
	}
		
	/**
	 * record the namenode info
	 * @author zhoujiang
	 */
	public class NameNodeInfo implements Writable{
		
		InetSocketAddress nodeaddr;
		protected int rpcport = 9000;
		protected int socketport = 9001;
		protected String hostname = "";
		protected NodeStyle style;
		// the current CRC value in this node for fsimage or editlog, bit-flipped
		protected int crc = 0 ;
		//if has formatted
		protected boolean isFormat = false;
				//if has reported
		protected boolean isReport = false;

			
		NameNodeInfo() {
			this.hostname = "";
			this.rpcport = 9000;
			this.socketport = 9001;
			this.isFormat = false;
			this.isReport = false;
		}
				
		public NameNodeInfo(String hostname, int rpcport, int socketport) {
			//LOG.info("namenode info hostname: " + hostname + ", rpcPort: " + rpcport + ",socketPort: " + socketport);
			this.hostname = hostname;
			this.rpcport = rpcport;
			this.socketport = socketport;
			this.isFormat = false;
			this.isReport = false;
		}

		public boolean getFormat() {
			return isFormat;
		}

		public void setFormat() {
			isFormat = true;
		}

		public boolean getReport() {
			return isReport;
		}
		
		public void setReport() {
			isReport = true;
		}
			
		public String getRpcAddress(){
			return hostname + ":" + rpcport;			
		}
		
		public String getSocketAddress() {
			return hostname + ":" + socketport;
		}
		
		public int getRpcPort() {
			return rpcport;
		}
		
		public int getSocketPort() {
			return socketport;
		}
		
		public String getHostName(){
			return hostname;
		}
		
		public String getHostPortName() {
			return hostname + ":" + socketport;
		}
		
		public void setCrc(int crc) {
			this.crc = crc;
		}
		
		public int getCrc(){
			return crc;
		}
		
		// implement write of Writable
		public void write(DataOutput out) throws IOException {
			Text.writeString(out, hostname);
			out.writeInt(rpcport);
			out.writeInt(socketport);
		}
		
		//implement readfields of Writable
		public void readFields(DataInput in) throws IOException {
			hostname = Text.readString(in);
			rpcport = in.readInt();
			socketport = in.readInt();
		}			
	}
	
	// read a NamenodeInfo
	public static NameNodeInfo read(DataInput in) throws IOException {
		PoolFile pf = new PoolFile();
		final NameNodeInfo node = pf.new NameNodeInfo();
		node.readFields(in);
		return node;		
	}	
		
	static public enum NodeStyle {
		LOCAL_NODE,
		REMOTE_NODE;
	}
	
	static public enum FileState {
		INTACT_FILE,
		CORRUPT_FILE;
	}
			
	public void addChild(NameNodeInfo node) {
		this.nodeInfos.add(node);		
	}
	
	public void removeChild(NameNodeInfo node) {
		//LOG.info("remove node " + node + "from poolfile" + name);
		this.nodeInfos.remove(node);
	}
	
	public void replaceChild(NameNodeInfo node){			
	}	
	
	public List<NameNodeInfo> getNodeInfos() {
		return nodeInfos;			
	}
	
	public File getLocalFile() {
		return new File(rootpath, name);
	}
	
	//return node dir name, like node1
	public String getNodeDirName() {
		int slash = 0;
		String tmpStr;
		slash = name.indexOf('/');
		tmpStr = name.substring(0, slash);
		return tmpStr; 
	}
	
	//return dir name, like current or checkpointXXX
	public String getDirName() {
		int slash = 0;
		String tmpStr = "";
		slash = name.indexOf('/');
		tmpStr = name.substring(slash+1);
		slash = tmpStr.indexOf('/');
		tmpStr = tmpStr.substring(0, slash);
		return tmpStr;
	}
	
	//return file name like fsimage or [checkpointXXX/]node1/current/fsimage
	public String getLastName() {
		int slash = 0;
		String tmpStr = "";
		slash = name.indexOf('/');
		tmpStr = name.substring(slash+1);
		slash = tmpStr.indexOf('/');
		tmpStr = tmpStr.substring(slash+1);
		return tmpStr;
	}
	
	//rename file name like fsimage.ckpt -> fsimage
	public void setName(String newName) {
		this.name = newName;
	}
	
	//return path name, such as se2/current/fsimage
	public String getName() {
		return name;
	}
	
	//return root, such as /mnt/localindex/pool
	public String getRootpath() {
		return rootpath;
	}
		
	//implement write of writable
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, rootpath);
		Text.writeString(out, name);
		out.writeBoolean(isDir);		
	}
	
	//implement readfields of writable
	public void readFields(DataInput in) throws IOException {
		rootpath = Text.readString(in);
		name = Text.readString(in);
		isDir = in.readBoolean();
	}
			
	/**
	 * overrides java.io.File.exists
	 * check for dirMap
	 */
	public boolean exists() {
		Map<String, PoolDirectory> mapd = 
			StorageClover.dirMap.get(getNodeDirName());
		if (mapd == null){ 
			//LOG.info("mapd does not exist :" + getNodeDirName());
			return false;
		}
		PoolDirectory pd = mapd.get(getDirName());
		if (pd == null) {
			//LOG.info("pd does not exist:" + getDirName());
			return false;
		}
		if (!isDir) {
			Map<String, PoolFile> poolFiles = pd.getPoolFiles();
			PoolFile tmppf = poolFiles.get(name);
			if (tmppf == null){ 
				//LOG.info("pf does not exist:" + name);
				return false;
			}
		}		
		//LOG.info("file : " + name + " exists in memory");
		return true;
	}
	
	/**
	 * try to get namenode list with the file name from dirMap 
	 * if first crate file, create namenode info with it
	 */
	public void tryFileNodeInfos() {
		//LOG.info("enter tryFileNodeInfos");
		Map<String, PoolDirectory> mapd = 
			StorageClover.dirMap.get(getNodeDirName());
		Map<String, PoolFile> poolFiles = null;		
		PoolFile tmppf = null;
		if ( mapd != null) {
			PoolDirectory pd = mapd.get(getDirName());
			if (pd != null && !isDir) {
				poolFiles = pd.getPoolFiles();
				tmppf = poolFiles.get(name);
				if (tmppf != null) {
					this.nodeInfos = tmppf.getNodeInfos();
					//LOG.info("no need add nodeinfo to " + tmppf.name + ", nodeInfos: "+ this.nodeInfos.size());
				}
			}
		}else {
			PoolFile pf = new PoolFile();
			//LOG.info("add mapd: " + getNodeDirName());
			mapd = new HashMap<String, PoolDirectory>();
			StorageClover.dirMap.put(getNodeDirName(), mapd);
			//LOG.info("add pool directory: " + getDirName());	
			PoolDirectory p = pf.new PoolDirectory(getDirName());
			mapd.put(getDirName(), p);
			poolFiles = p.getPoolFiles();
			tmppf = poolFiles.get(name);
		}	

		//if no namenode associated with file, add namenode
		//first add local node, then random select from nodes ring
		if (this.nodeInfos.size() == 0) {
			Random r = new Random();
			boolean isAdd = true;
			int index = 0;
			int num = StorageClover.getPoolNodes().size();
		 	//LOG.info("need select nodeinfos in " + num + " nodes");	
			this.nodeInfos.add(NameNode.client.storage.localnode);
			//LOG.info("nodeinfos size : " + this.nodeInfos.size() );
			index = r.nextInt();
			if (index < 0) index = 0;
			for (int i=0; i< POOLFILE_REPLICA_NUM; i++) {
				index = index % num;
				NameNodeInfo child = StorageClover.getPoolNodes().get(index);
				isAdd = true;
				for (int j=0; j< this.nodeInfos.size(); j++) {
					if (this.nodeInfos.get(j).getHostName().equalsIgnoreCase(child.getHostName())){
						isAdd = false;
					}
				}
				if (isAdd) {
					//LOG.info("add child" + index + ", hostname: " + child.getHostName() + ", rpcport: " + child.getRpcPort() + ", socketport: "
					//	+ child.getSocketPort());
					addChild(child);
				}
				index++;
			}
		}
		if (tmppf == null) {
			//LOG.info("add poolfile to mapd, name:" + name);
			poolFiles.put(name, this);
		}
	
	}
	
	
	/**
	 * create dirs prior to file name
	 * like ${root}/node1/fsimage/
	 */
	public void createdirs() throws IOException {
		String pathname = getLocalFile().getAbsolutePath();
		String dirname = "";
		int lastSlash = pathname.lastIndexOf('/');
		dirname = pathname.substring(0, lastSlash);
		File dir = new File(dirname);
		if(!dir.exists()){
			//LOG.info("need create dirs first: " + dirname);
			if(!dir.mkdirs())
				throw new IOException("Cannot create directory " + dir);
		}
	}
	
	/**
	 * rename file to dest file
	 */
	public boolean renameTo(File dest) {
		PoolFile destF = (PoolFile)dest;
		try {
			NameNode.client.rename(this, destF, NameNode.client.storage.localnode);
		} catch (IOException ie){
			return false;
		}
		return true;
	}
	
	/**
	 * only delete file 
	 */
	public boolean delete() {
		assert NameNode.client !=null : "poolclient is null";
		return NameNode.client.delete(this);
		
	}
	
	/**
	 * java.io.File.mkdir
	 * broadcast to all nodes to modify dirMap in memory
	 * and create directory
	 */
	public boolean mkdir() {
		NameNode.client.mkdir(this);
		return this.getLocalFile().exists();
	}
	
	public boolean mkdirs() {
		NameNode.client.mkdir(this);
		return this.getLocalFile().exists();
	}
		
	@Override
	public long length() {
		return getLocalFile().length();
	}

	/**
	 * java.io.File.createNewFile
	 * only local
	 */
	public boolean createNewFile() throws IOException {
		return this.getLocalFile().createNewFile();
	}
	
	/**
	 * test whether the file can be read
	 * only local
	 */
	public boolean canRead() {
		return this.getLocalFile().canRead();		
	}
	
	
}
