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
package org.apache.hadoop.hdfs;

import static org.apache.hadoop.hdfs.protocol.DataTransferProtocol.Op.BLOCK_CHECKSUM;
import static org.apache.hadoop.hdfs.protocol.DataTransferProtocol.Status.ERROR_ACCESS_TOKEN;
import static org.apache.hadoop.hdfs.protocol.DataTransferProtocol.Status.SUCCESS;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys.FSOperations;
import org.apache.hadoop.hdfs.SendingData.process_data_request_proto;
import org.apache.hadoop.hdfs.SendingData.process_data_response_proto;
//import org.apache.hadoop.hdfs.SendingData.process_data_service.BlockingInterface;
//import org.apache.hadoop.hdfs.server.protocol.CloverProtocol;

import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.util.*;
 
//protocol buffer
import com.google.protobuf.*;
import com.googlecode.protobuf.socketrpc.*;
  
/********************************************************
 * DFSClient can connect to a Hadoop Filesystem and 
 * perform basic file tasks.  It uses the ClientProtocol
 * to communicate with a NameNode daemon, and connects 
 * directly to DataNodes to read/write block data.
 *
 * Hadoop DFS users should obtain an instance of 
 * DistributedFileSystem, which uses DFSClient to handle
 * filesystem tasks.
 *
 ********************************************************/
@InterfaceAudience.Private
@SuppressWarnings("deprecation")
public class DFSClient implements FSConstants, java.io.Closeable {
	public static final Log LOG = LogFactory.getLog(DFSClient.class);
	public static final long SERVER_DEFAULTS_VALIDITY_PERIOD = 60 * 60 * 1000L; // 1
 
	
 	public static final int MAX_BLOCK_ACQUIRE_FAILURES = 3;
	static final int TCP_WINDOW_SIZE = 128 * 1024; // 128 KB
	final ClientProtocol namenode;
	private final ClientProtocol rpcNamenode;
	final UserGroupInformation ugi;
	volatile boolean clientRunning = true;
	private volatile FsServerDefaults serverDefaults;
	private volatile long serverDefaultsLastUpdate;
	Random r = new Random();
	final String clientName;
	final LeaseChecker leasechecker = new LeaseChecker();
	Configuration conf;
	long defaultBlockSize;
	private short defaultReplication;
	SocketFactory socketFactory;
	int socketTimeout;
	final int writePacketSize;
	final FileSystem.Statistics stats;
	final int hdfsTimeout; // timeout value for a DFS operation.
 
	/**
	 * getDnn** parse the configuration file to find available namenodes
	 * 
	 * @param conf
	 * @return
	 */
	  
	 
  /**
   * The locking hierarchy is to first acquire lock on DFSClient object, followed by 
   * lock on leasechecker, followed by lock on an individual DFSOutputStream.
   */
	public static ClientProtocol createNamenode(Configuration conf) throws IOException {
		return createNamenode(NameNode.getAddress(conf), conf);
	}

  public static ClientProtocol createNamenode( InetSocketAddress nameNodeAddr,
      Configuration conf) throws IOException {
    return createNamenode(createRPCNamenode(nameNodeAddr, conf,
        UserGroupInformation.getCurrentUser()));
    
  }

  private static ClientProtocol createRPCNamenode(InetSocketAddress nameNodeAddr,
      Configuration conf, UserGroupInformation ugi) 
    throws IOException {
    return (ClientProtocol)RPC.getProxy(ClientProtocol.class,
        ClientProtocol.versionID, nameNodeAddr, ugi, conf,
        NetUtils.getSocketFactory(conf, ClientProtocol.class));
  }

  private static ClientProtocol createNamenode(ClientProtocol rpcNamenode)
    throws IOException {
    RetryPolicy createPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
        5, LEASE_SOFTLIMIT_PERIOD, TimeUnit.MILLISECONDS);
    
    Map<Class<? extends Exception>,RetryPolicy> remoteExceptionToPolicyMap =
      new HashMap<Class<? extends Exception>, RetryPolicy>();
    remoteExceptionToPolicyMap.put(AlreadyBeingCreatedException.class, createPolicy);

    Map<Class<? extends Exception>,RetryPolicy> exceptionToPolicyMap =
      new HashMap<Class<? extends Exception>, RetryPolicy>();
    exceptionToPolicyMap.put(RemoteException.class, 
        RetryPolicies.retryByRemoteException(
            RetryPolicies.TRY_ONCE_THEN_FAIL, remoteExceptionToPolicyMap));
    RetryPolicy methodPolicy = RetryPolicies.retryByException(
        RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
    Map<String,RetryPolicy> methodNameToPolicyMap = new HashMap<String,RetryPolicy>();
    
    methodNameToPolicyMap.put("create", methodPolicy);

    return (ClientProtocol) RetryProxy.create(ClientProtocol.class,
        rpcNamenode, methodNameToPolicyMap);
  }

  static ClientDatanodeProtocol createClientDatanodeProtocolProxy (
      DatanodeID datanodeid, Configuration conf) throws IOException {
    InetSocketAddress addr = NetUtils.createSocketAddr(
      datanodeid.getHost() + ":" + datanodeid.getIpcPort());
    if (ClientDatanodeProtocol.LOG.isDebugEnabled()) {
      ClientDatanodeProtocol.LOG.info("ClientDatanodeProtocol addr=" + addr);
    }
    return (ClientDatanodeProtocol)RPC.getProxy(ClientDatanodeProtocol.class,
        ClientDatanodeProtocol.versionID, addr, conf);
  }
        
  /**
   * Same as this(NameNode.getAddress(conf), conf);
   * @see #DFSClient(InetSocketAddress, Configuration)
   * @deprecated Deprecated at 0.21
   */
  @Deprecated
  public DFSClient(Configuration conf) throws IOException {
    this(NameNode.getAddress(conf), conf);
  }

  /**
   * Same as this(nameNodeAddr, conf, null);
   * @see #DFSClient(InetSocketAddress, Configuration, org.apache.hadoop.fs.FileSystem.Statistics)
   */
  public DFSClient(InetSocketAddress nameNodeAddr, Configuration conf
      ) throws IOException {
    this(nameNodeAddr, conf, null);
  }

  /**
   * Same as this(nameNodeAddr, null, conf, stats);
   * @see #DFSClient(InetSocketAddress, ClientProtocol, Configuration, org.apache.hadoop.fs.FileSystem.Statistics) 
   */
  public DFSClient(InetSocketAddress nameNodeAddr, Configuration conf,
                   FileSystem.Statistics stats)
    throws IOException {
    this(nameNodeAddr, null, conf, stats);
  }

	// add by tacitus
	final long dfsclientTimeout = 1000 * 60 * 10l; // 10 minutes;

	/**
	 * Create a new DFSClient connected to the given nameNodeAddr or
	 * rpcNamenode. Exactly one of nameNodeAddr or rpcNamenode must be null.
	 */
	
	
	// add by wangyouwei
	static String[] hosts = null;
  	static ClientProtocol[] namenode_array = null;
	int pb_socket_rpc_port  = -1;
 	
	ExecutorService thread_pool = null;
	//RpcController[] controllers = null;
	static RpcController controller = null;
	static RpcChannel[] channels = null;
	static SendingData.process_data_service[] non_blocking_services = null;
	

	DFSClient(InetSocketAddress nameNodeAddr, ClientProtocol rpcNamenode,
      Configuration conf, FileSystem.Statistics stats)
    throws IOException {
    this.conf = conf;
    this.stats = stats;
    this.socketTimeout = conf.getInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 
                                     HdfsConstants.READ_TIMEOUT);
    this.socketFactory = NetUtils.getSocketFactory(conf, ClientProtocol.class);
    // dfs.write.packet.size is an internal config variable
    this.writePacketSize = conf.getInt(DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, 
		                       DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT);
    // The hdfsTimeout is currently the same as the ipc timeout 
    this.hdfsTimeout = Client.getTimeout(conf);

    this.ugi = UserGroupInformation.getCurrentUser();
    
    String taskId = conf.get("mapred.task.id");
    if (taskId != null) {
      this.clientName = "DFSClient_" + taskId; 
    } else {
      this.clientName = "DFSClient_" + r.nextInt();
    }
    defaultBlockSize = conf.getLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    defaultReplication = (short) conf.getInt("dfs.replication", 3);

		if (nameNodeAddr != null && rpcNamenode == null) {
			this.rpcNamenode = createRPCNamenode(nameNodeAddr, conf, ugi);
			this.namenode = createNamenode(this.rpcNamenode);
		} else if (nameNodeAddr == null && rpcNamenode != null) {
			//This case is used for testing.
			this.namenode = this.rpcNamenode = rpcNamenode;
		} else {
      throw new IllegalArgumentException(
          "Expecting exactly one of nameNodeAddr and rpcNamenode being null: "
          + "nameNodeAddr=" + nameNodeAddr + ", rpcNamenode=" + rpcNamenode);
    }


		
		//initialize the namenode array
		// <name>dfs.dnn.list</name>
		// <value>hdfs://anode1:20300/,hdfs://anode2:20300/,hdfs://anode3:20300/,hdfs://anode4:20300/</value>
		//============================================================================
  		try {
  			String namenode_array_config = conf.get(DFSConfigKeys.DFS_DNN_LIST_KEY);
  			String port_config = conf.get(DFSConfigKeys.PROTOCOL_BUFFER_SOCKET_RPC_PORT);
			if (namenode_array_config == null) {
				// sorry, null value read from configuration:
				// maybe you are running on the FUSE mode:
				Configuration xconf = new Configuration();
				xconf.addResource(new Path(System.getenv("HADOOP_HOME")+ "/conf/core-site.xml"));
				xconf.addResource(new Path(System.getenv("HADOOP_HOME")+ "/conf/hdfs-site.xml"));
				//reload the configuration
				namenode_array_config = xconf.get(DFSConfigKeys.DFS_DNN_LIST_KEY);
				port_config = xconf.get(DFSConfigKeys.PROTOCOL_BUFFER_SOCKET_RPC_PORT);
				
				//FileWriter fw = new FileWriter("/tmp/blackbox.log");
				//fw.write(conf.toString());
				/*
				if (namenode_array_config == null) {
					final String s = "DFSClient.288:namenode_array_config is null, check your conf...\n";
					String dnnlist = xconf.get(DFSConfigKeys.DFS_DNN_LIST_KEY);
					dnnlist = "\ndnnlist from xconf: " + dnnlist + "\n";
					String port = xconf
							.get(DFSConfigKeys.PROTOCOL_BUFFER_SOCKET_RPC_PORT);
					port = "\nport from xconf: " + port + "\n";
					//fw.write(dnnlist, 0, dnnlist.length());
					//fw.write(port, 0, port.length());
					//fw.write(s, 0, s.length());
					//fw.flush();
				}*/
				//fw.close();
			}
			
			 
 			this.pb_socket_rpc_port = Integer.valueOf(port_config);
			String[] namenode_array_strings = namenode_array_config.split(",");  			
 			hosts = new String[namenode_array_strings.length];
			namenode_array = new ClientProtocol[namenode_array_strings.length];
			int cursor = 0;
			for (String namenode_uri : namenode_array_strings) {
				final URI uri = new URI(namenode_uri);
				hosts[cursor] = uri.getHost();
				//DFSClient.LOG.info("[dbg] current idx #" + cursor + " -> " + hosts[cursor]);
				InetSocketAddress namenode_addr = new InetSocketAddress(hosts[cursor], uri.getPort());
 				namenode_array[cursor] = createNamenode(createRPCNamenode(namenode_addr, conf, ugi));
 				cursor++;

			}
		} catch (Exception e) {
			DFSClient.LOG.error(StringUtils.stringifyException(e));
			throw new IOException(e);
		}
	
		//initialize the network data structure 
		thread_pool = Executors.newFixedThreadPool(20);
		controller = new SocketRpcController();
		//controllers = new RpcController[hosts.length];
		channels = new RpcChannel[hosts.length];
		non_blocking_services = new SendingData.process_data_service[hosts.length];		
		//DFSClient.LOG.info("[dbg] DFSClient->pb_socket_rpc_port: " + this.pb_socket_rpc_port);
		for (int i = 0; i < hosts.length; i++) {
			final String host = hosts[i];
			RpcConnectionFactory connectionFactory = 
					SocketRpcConnectionFactories.createRpcConnectionFactory(host, this.pb_socket_rpc_port);
			channels[i] = RpcChannels.newRpcChannel(connectionFactory, thread_pool);
			non_blocking_services[i] = SendingData.process_data_service.newStub(channels[i]);
			//controllers[i] = new SocketRpcController();
			
 		}
		 
	}
	

	
	static public ClientProtocol get_namenode_for_parent_path(String src) throws IOException {
		String parent_path = (new Path(src)).getParent().toString();
		long parent_src_uid = DFSClient.get_uid(parent_path); //XXX this could be -1
		if (parent_src_uid < 0) {
			//DFSClient.LOG.error("no parent found for path: " + src);
			//return null;
			throw new IOException("no parent found for path: " + src);
		}
		int parent_host_idx = DFSClient.map_uid_to_host_idx(parent_src_uid); 
		return namenode_array[parent_host_idx];
	}
	
	


	static final int CONNECT_TIMEOUT = 5000;

	/**
	 * Return the number of times the client should go back to the namenode to
	 * retrieve block locations when reading.
	 */
  int getMaxBlockAcquireFailures() {
    return conf.getInt("dfs.client.max.block.acquire.failures",
                       MAX_BLOCK_ACQUIRE_FAILURES);
  }

  /**
   * Return the timeout that clients should use when writing to datanodes.
   * @param numNodes the number of nodes in the pipeline.
   */
  int getDatanodeWriteTimeout(int numNodes) {
    int confTime =
        conf.getInt("dfs.datanode.socket.write.timeout",
                    HdfsConstants.WRITE_TIMEOUT);

    return (confTime > 0) ?
      (confTime + HdfsConstants.WRITE_TIMEOUT_EXTENSION * numNodes) : 0;
  }

  int getDatanodeReadTimeout(int numNodes) {
    return socketTimeout > 0 ?
        (HdfsConstants.READ_TIMEOUT_EXTENSION * numNodes +
        socketTimeout) : 0;
  }

  void checkOpen() throws IOException {
    if (!clientRunning) {
      IOException result = new IOException("Filesystem closed");
      throw result;
    }
  }

  /**
   * Close the file system, abandoning all of the leases and files being
   * created and close connections to the namenode.
   */
  public synchronized void close() throws IOException {
    if(clientRunning) {
      leasechecker.close();
      clientRunning = false;
      try {
        leasechecker.interruptAndJoin();
      } catch (InterruptedException ie) {
      }
  
      // close connections to the namenode
      RPC.stopProxy(rpcNamenode);
    }
  }

  /**
   * Get the default block size for this cluster
   * @return the default block size in bytes
   */
  public long getDefaultBlockSize() {
    return defaultBlockSize;
  }
    
  public long getBlockSize(String f) throws IOException {
    try {
			
			Path parentPath = (new Path(f)).getParent();
			long parent_uid = get_uid(parentPath.toString());
			if (parent_uid < 0) {
				throw new IOException("parent dir of " + f + " does not exist");
			}
			int parent_host_idx = DFSClient.map_uid_to_host_idx(parent_uid);
			return namenode_array[parent_host_idx].getPreferredBlockSize(f);
			//long longReg = getNN("").getPreferredBlockSize(f);
 			
		} catch (IOException ie) {
			throw ie;
		}
	}

	/**
	 * Get server default values for a number of configuration params.
	 */
  public FsServerDefaults getServerDefaults() throws IOException {
    long now = System.currentTimeMillis();
		if (now - serverDefaultsLastUpdate > SERVER_DEFAULTS_VALIDITY_PERIOD) {
			// serverDefaults = getNN("").getServerDefaults();
			serverDefaults = namenode_array[0].getServerDefaults();
			serverDefaultsLastUpdate = now;
			return serverDefaults;

    }
    return serverDefaults;
  }

  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
		return namenode_array[0].getDelegationToken(renewer);
 		//return getNN("").getDelegationToken(renewer);	
	}

	public long renewDelegationToken(Token<DelegationTokenIdentifier> token) throws InvalidToken, IOException {
		try {
			return namenode_array[0].renewDelegationToken(token);
			//return getNN("").renewDelegationToken(token);
		} catch (RemoteException re) {
			throw re.unwrapRemoteException(InvalidToken.class, AccessControlException.class);
		}
	}

  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws InvalidToken, IOException {
    try {
			namenode_array[0].cancelDelegationToken(token);
			//getNN("").cancelDelegationToken(token);
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(InvalidToken.class,
                                     AccessControlException.class);
    }
  }

  /**
   * Report corrupt blocks that were discovered by the client.
   */
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
		//boardcasting...
		for (int i = 0; i < namenode_array.length; i++) {
			namenode_array[i].reportBadBlocks(blocks);
		}
		 
 		//getNN("").reportBadBlocks(blocks);
 
  }
  
  public short getDefaultReplication() {
    return defaultReplication;
  }

	
	public static LocatedBlocks callGetBlockLocations(String src, long start, long length) 
			throws IOException, UnresolvedLinkException {
		//src must be a file;
		try {
			Path parentPath = (new Path(src)).getParent();
			long parent_uid = get_uid(parentPath.toString());
			if (parent_uid < 0) {
				return null;
			}
			int parent_host_idx = DFSClient.map_uid_to_host_idx(parent_uid);
			return namenode_array[parent_host_idx].getBlockLocations(src, start, length);
			//return namenode.getNN(src).getBlockLocations(src, start, length);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     UnresolvedPathException.class);
    }
  }
	
	public static LocatedBlocks callGetBlockLocations(ClientProtocol namenode,
			String src, long start, long length) throws IOException, UnresolvedLinkException {
		return DFSClient.callGetBlockLocations(src, start, length);
	}

  /**
   * Get block location info about file
   * 
   * getBlockLocations() returns a list of hostnames that store 
   * data for a specific file region.  It returns a set of hostnames
   * for every block within the indicated region.
   *
   * This function is very useful when writing code that considers
   * data-placement when performing operations.  For example, the
   * MapReduce system tries to schedule tasks on the same machines
   * as the data-block the task processes. 
   */
  public BlockLocation[] getBlockLocations(String src, long start, 
    long length) throws IOException, UnresolvedLinkException {	  
    LocatedBlocks blocks = callGetBlockLocations(namenode, src, start, length);
    if (blocks == null) {
      return new BlockLocation[0];
    }
    int nrBlocks = blocks.locatedBlockCount();
    BlockLocation[] blkLocations = new BlockLocation[nrBlocks];
    int idx = 0;
    for (LocatedBlock blk : blocks.getLocatedBlocks()) {
      assert idx < nrBlocks : "Incorrect index";
      DatanodeInfo[] locations = blk.getLocations();
      String[] hosts = new String[locations.length];
      String[] names = new String[locations.length];
      String[] racks = new String[locations.length];
      for (int hCnt = 0; hCnt < locations.length; hCnt++) {
        hosts[hCnt] = locations[hCnt].getHostName();
        names[hCnt] = locations[hCnt].getName();
        NodeBase node = new NodeBase(names[hCnt], 
                                     locations[hCnt].getNetworkLocation());
        racks[hCnt] = node.toString();
      }
      blkLocations[idx] = new BlockLocation(names, hosts, racks,
                                            blk.getStartOffset(),
                                            blk.getBlockSize());
      idx++;
    }
    return blkLocations;
  }

  public DFSInputStream open(String src) 
      throws IOException, UnresolvedLinkException {
    return open(src, conf.getInt("io.file.buffer.size", 4096), true, null);
  }

  /**
   * Create an input stream that obtains a nodelist from the
   * namenode, and then reads from all the right places.  Creates
   * inner subclass of InputStream that does the right out-of-band
   * work.
   * @deprecated Use {@link #open(String, int, boolean)} instead.
   */
  @Deprecated
  public DFSInputStream open(String src, int buffersize, boolean verifyChecksum,
                             FileSystem.Statistics stats)
      throws IOException, UnresolvedLinkException {
    return open(src, buffersize, verifyChecksum);
  }
  

  /**
   * Create an input stream that obtains a nodelist from the
   * namenode, and then reads from all the right places.  Creates
   * inner subclass of InputStream that does the right out-of-band
   * work.
   */
  public DFSInputStream open(String src, int buffersize, boolean verifyChecksum)
      throws IOException, UnresolvedLinkException {
    checkOpen();
    //    Get block info from namenode
    return new DFSInputStream(this, src, buffersize, verifyChecksum);
  }

  /**
   * Create a new dfs file and return an output stream for writing into it. 
   * 
   * @param src stream name
   * @param overwrite do not check for file existence if true
   * @return output stream
   * @throws UnresolvedLinkException if a symlink is encountered in src.
   * @throws IOException
   */
  public OutputStream create(String src, boolean overwrite) 
      throws IOException, UnresolvedLinkException {
    return create(src, overwrite, defaultReplication, defaultBlockSize, null);
  }
    
  /**
   * Create a new dfs file and return an output stream for writing into it
   * with write-progress reporting. 
   * 
   * @param src stream name
   * @param overwrite do not check for file existence if true
   * @return output stream
   * @throws UnresolvedLinkException if a symlink is encountered in src.
   * @throws IOException
   */
  
  public OutputStream create(String src, 
                             boolean overwrite,
                             Progressable progress)
      throws IOException, UnresolvedLinkException {
    return create(src, overwrite, defaultReplication, defaultBlockSize, null);
  }
    
  /**
   * Create a new dfs file with the specified block replication 
   * and return an output stream for writing into the file.  
   * 
   * @param src stream name
   * @param overwrite do not check for file existence if true
   * @param replication block replication
   * @return output stream
   * @throws UnresolvedLinkException if a symlink is encountered in src.
   * @throws IOException
   */
  
  public OutputStream create(String src, 
                             boolean overwrite, 
                             short replication,
                             long blockSize)
      throws IOException, UnresolvedLinkException {
    return create(src, overwrite, replication, blockSize, null);
  }

  /**
   * Get the namenode associated with this DFSClient object
   * @return the namenode associated with this DFSClient object
   */
  public ClientProtocol getNamenode() {
    return namenode;
  }
  
  
  /**
   * Create a new dfs file with the specified block replication 
   * with write-progress reporting and return an output stream for writing
   * into the file.  
   * 
   * @param src stream name
   * @param overwrite do not check for file existence if true
   * @param replication block replication
   * @return output stream
   * @throws UnresolvedLinkException if a symlink is encountered in src.
   * @throws IOException
   */
  public OutputStream create(String src, 
                             boolean overwrite, 
                             short replication,
                             long blockSize,
                             Progressable progress)
      throws IOException, UnresolvedLinkException {
    return create(src, overwrite, replication, blockSize, progress,
        conf.getInt("io.file.buffer.size", 4096));
  }
  /**
   * Call
   * {@link #create(String,FsPermission,EnumSet,short,long,Progressable,int)}
   * with default permission.
   * @see FsPermission#getDefault()
   */
  public OutputStream create(String src,
      boolean overwrite,
      short replication,
      long blockSize,
      Progressable progress,
      int buffersize)
      throws IOException, UnresolvedLinkException {
    return create(src, FsPermission.getDefault(),
        overwrite ? EnumSet.of(CreateFlag.OVERWRITE) : EnumSet.of(CreateFlag.CREATE), 
        replication, blockSize, progress, buffersize);
  }

  /**
   * Call
   * {@link #create(String,FsPermission,EnumSet,boolean,short,long,Progressable,int)}
   * with createParent set to true.
   */
  public OutputStream create(String src, 
      FsPermission permission,
      EnumSet<CreateFlag> flag, 
      short replication,
      long blockSize,
      Progressable progress,
      int buffersize)
      throws IOException, UnresolvedLinkException {
    return create(src, permission, flag, true,
        replication, blockSize, progress, buffersize);
  }

  /**
   * Create a new dfs file with the specified block replication 
   * with write-progress reporting and return an output stream for writing
   * into the file.  
   * 
   * @param src stream name
   * @param permission The permission of the directory being created.
   * If permission == null, use {@link FsPermission#getDefault()}.
   * @param flag do not check for file existence if true
   * @param createParent create missing parent directory if true
   * @param replication block replication
   * @return output stream
   * @throws IOException
   * @throws UnresolvedLinkException if src contains a symlink. 
   * @see ClientProtocol#create(String, FsPermission, String, EnumSetWritable, boolean, short, long)
   */
  public OutputStream create(String src, 
                             FsPermission permission,
                             EnumSet<CreateFlag> flag, 
                             boolean createParent,
                             short replication,
                             long blockSize,
                             Progressable progress,
                             int buffersize)
    throws IOException, UnresolvedLinkException {
    checkOpen();
    if (permission == null) {
      permission = FsPermission.getDefault();
    }
    FsPermission masked = permission.applyUMask(FsPermission.getUMask(conf));
    LOG.debug(src + ": masked=" + masked);
   
    //add by dr.who: create parent
    if (createParent) {
 		String src_parent = (new Path(src)).getParent().toString();
 		if (DFSClient.get_uid(src_parent) < 0) { // no such parent
 			if (!this.mkdirs(src_parent, masked, true)) {
 				return null;
			}else {
 			}
		}else{
 		}
	}
    //============================================================
    
    OutputStream result = new DFSOutputStream(this, src, masked,
        flag, createParent, replication, blockSize, progress, buffersize,
        conf.getInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 
                    DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT));
    leasechecker.put(src, result);
     return result;
  }
  
  /**
   * Same as {{@link #create(String, FsPermission, EnumSet, short, long,
   *  Progressable, int)}   except that the permission
   *   is absolute (ie has already been masked with umask.
   */
  public OutputStream primitiveCreate(String src, 
                             FsPermission absPermission,
                             EnumSet<CreateFlag> flag,
                             boolean createParent,
                             short replication,
                             long blockSize,
                             Progressable progress,
                             int buffersize,
                             int bytesPerChecksum)
      throws IOException, UnresolvedLinkException {
    checkOpen();
    
    //add by dr.who: create parent
    if (createParent) {
		String src_parent = (new Path(src)).getParent().toString();
		if (get_uid(src_parent) == -1) { // no such parent
			if (!this.mkdirs(src_parent, absPermission, true)) {
				return null;
			}
		}
	}
    //============================================================
    
    OutputStream result = new DFSOutputStream(this, src, absPermission,
        flag, createParent, replication, blockSize, progress, buffersize,
        bytesPerChecksum);
    leasechecker.put(src, result);
    return result;
  }
  
  /**
   * Creates a symbolic link.
   * 
   * @see ClientProtocol#createSymlink(String, String,FsPermission, boolean) 
   */
  public void createSymlink(String target, String link, boolean createParent)
      throws IOException, UnresolvedLinkException {
    try {
      FsPermission dirPerm = 
          FsPermission.getDefault().applyUMask(FsPermission.getUMask(conf));
      
      boolean parent_exist = true;
      String link_parent = (new Path(link)).getParent().toString();
      long link_parent_diruid = get_uid(link_parent); 
      if (link_parent_diruid < 0) {
    	 parent_exist = false;
      }
      
      //add by dr.who: create parent
      if (createParent) {
    	  if (parent_exist) {
    		  //ignore
    	  }else {
    		  if (!this.mkdirs(link_parent, dirPerm, true)) {
    				return ;
    		  }else {
    			  link_parent_diruid =  DFSClient.get_uid(link_parent);
    		  }
    	  }
  	  }else {
  		  //parent doesn't exist and forbid creating parent;
  		  return;
  	  }
      final int namenode_idx = DFSClient.map_uid_to_host_idx(link_parent_diruid);
      final ClientProtocol rpc_target = namenode_array[namenode_idx];
      rpc_target.createSymlink(target, link, dirPerm, createParent);
      
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     NSQuotaExceededException.class, 
                                     DSQuotaExceededException.class,
                                     FileAlreadyExistsException.class, 
                                     UnresolvedPathException.class);
    }
  }

  /**
   * Resolve the *first* symlink, if any, in the path.
   * 
   * @see ClientProtocol#getLinkTarget(String)
   */
  public String getLinkTarget(String path) throws IOException { 
    checkOpen();
    try {
			Path parentPath = (new Path(path)).getParent();
			long parent_uid = get_uid(parentPath.toString());
			if (parent_uid < 0) {
				return null;
			}
			int parent_host_idx = DFSClient.map_uid_to_host_idx(parent_uid);
			return namenode_array[parent_host_idx].getLinkTarget(path);
			
    } catch (RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class);
    }
  }
  /**
   * Append to an existing HDFS file.  
   * 
   * @param src file name
   * @param buffersize buffer size
   * @param progress for reporting write-progress
   * @return an output stream for writing into the file
   * @throws IOException
   * @throws UnresolvedLinkException if the path contains a symlink.
   * @see ClientProtocol#append(String, String)
   */
  OutputStream append(String src, int buffersize, Progressable progress)
      throws IOException, UnresolvedLinkException {
    checkOpen();
    HdfsFileStatus stat = null;
    LocatedBlock lastBlock = null;
    try {
      stat = getFileInfo(src);
			Path parentPath = (new Path(src)).getParent();
			long parent_uid = get_uid(parentPath.toString());
			if (parent_uid < 0) {
				return null;
			}
			int parent_host_idx = DFSClient.map_uid_to_host_idx(parent_uid);
 			lastBlock = (LocatedBlock) namenode_array[parent_host_idx].append(src, clientName);
			//lastBlock = getNN(src).append(src, clientName);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(FileNotFoundException.class,
                                     AccessControlException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class,
                                     UnresolvedPathException.class);
    }
    OutputStream result = new DFSOutputStream(this, src, buffersize, progress,
        lastBlock, stat, conf.getInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 
                                     DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT));
    leasechecker.put(src, result);
    return result;
  }

  /**
   * Set replication for an existing file.
   * 
   * @see ClientProtocol#setReplication(String, short)
   * @param replication
   * @throws IOException
   * @return true is successful or false if file does not exist 
   */
  public boolean setReplication(String src, 
                                short replication)
      throws IOException, UnresolvedLinkException {
    try {			
 			Path parentPath = (new Path(src)).getParent();
			long parent_uid = get_uid(parentPath.toString());
			if (parent_uid < 0) {
				throw new IOException("parent dir of " + src + " does not exist");
			}
			int parent_host_idx = DFSClient.map_uid_to_host_idx(parent_uid);
			return namenode_array[parent_host_idx].setReplication(src, replication);
 			//getNN(src).setReplication(src, replication);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class,
                                     UnresolvedPathException.class);
    }
  }


  /**
   * Rename file or directory.
   * See {@link ClientProtocol#rename(String, String)}.
   * @deprecated Use {@link #rename(String, String, Options.Rename...)} instead.
   */
  @Deprecated
  public boolean rename(String src, String dst) 
      throws IOException, UnresolvedLinkException {
	checkOpen();
		boolean result = true;
		DFSClient.LOG.info("[dbg] hive->dfsclient.rename: ");
		DFSClient.LOG.info("[dbg] src: " + src);
		DFSClient.LOG.info("[dbg] dst: " + dst);
		 
		
		HdfsFileStatus src_file_status = this.getFileInfo(src);
		if (src_file_status == null) {
			//LOG.info("[dbg] dfsclient.rename: getting src HdfsFileStatus [failed]: " + src);
			return false;
		}
		
		// calculate the final path
		HdfsFileStatus dst_file_status = this.getFileInfo(dst);
		String final_dst_path = null;
		if (dst_file_status == null) {
			//dst file is null
			//test its parent...
			String dst_parent_path = (new Path(dst)).getParent().toString();
			HdfsFileStatus dst_parent_path_file_status = this.getFileInfo(dst_parent_path);
			if (dst_parent_path_file_status == null) {
				// illegal operation
				return false;
			}else if(! dst_parent_path_file_status.isDir()){
				// illegal operation
				return false;
			}else {
				// move under a directory, and the final name is dst;
				 final_dst_path =  dst;
			}
		}else if (dst_file_status.isDir()) {
			// move the src file under the dst dir
			String[] path_component = src.split("/");
			String src_file_name = path_component[path_component.length - 1 ];
			if (dst.endsWith("/")) {
				final_dst_path = dst + src_file_name;
			}else {
				final_dst_path = dst + "/" + src_file_name; 
			}
			
		}else {
			// rename src
			// dst ia an existing file, exit...h
			return false;
		}
		
		
		if (src_file_status.isDir()) {
			//src is a dir
			// step 1: rename uid
			
			//boardcasting
			//use the #0 host as the coordinator
			final int coordinator_idx  = 0;
			String transaction_id = this.generate_uuid();
			{// step 2: assigning the coordinator
				DataOutputBuffer outputBuffer = new DataOutputBuffer();
				//outputBuffer.writeInt(FSOperations.COORDINATOR.ordinal());
				outputBuffer.writeBytes(transaction_id + DFSConfigKeys.new_line);
				outputBuffer.writeInt(hosts.length); // cohorts = parent + child;
				for (int i = 0; i < hosts.length; i++) {
					outputBuffer.writeInt(i);
				}
				
				pb_non_blocking_socket_rpc(coordinator_idx, 
						FSOperations.COORDINATOR.ordinal(), 
						outputBuffer.getData());
				outputBuffer.close();
				//LOG.info("[dbg] DFSClient.rename[dir] assign coordinator......[done] ");
			}
			
			
			//boardcasting to cohorts
			{
				//step 2: boardcasting operations
				final int[] target_host_array = new int[hosts.length];
				final long[] opcode = new long[hosts.length];
				DataOutputBuffer[] msgs = new DataOutputBuffer[hosts.length];
				for (int i = 0; i < target_host_array.length; i++) {
					target_host_array[i] = i;
					opcode[i] = FSOperations.RENAME_DIR.ordinal();
					msgs[i] = new DataOutputBuffer();
					msgs[i].writeBytes(transaction_id + DFSConfigKeys.new_line);
					msgs[i].writeInt(coordinator_idx);
					msgs[i].writeBytes(src + DFSConfigKeys.new_line);
					msgs[i].writeBytes(final_dst_path + DFSConfigKeys.new_line);
					msgs[i].writeShort(src_file_status.getPermission().toShort());
					 
 				}
				
				Object[] results = this.parallel_pb_socket_rpc(target_host_array, opcode, msgs);
				final DataInputBuffer dib = new DataInputBuffer();
				for (int i = 0; i < hosts.length; i++) {
					byte[] data = ((ByteString)results[i]).toByteArray();
 					dib.reset(data, data.length);
					result = result && dib.readBoolean();
				}
				dib.close();
			}
			
			//DFSClient.LOG.info("[dbg] dfsclient.rename: rename uid......[start]");
			result = this.rename_uid(src, final_dst_path); 
			//DFSClient.LOG.info("[dbg] dfsclient.rename: rename uid: return " + result);
			return  result;
			
		
			
		}else {
			//src is a file
			//start operation: rename_file
			String src_parent = (new Path(src)).getParent().toString();
			String dst_parent = (new Path(final_dst_path)).getParent().toString();
			final long src_parent_uid = get_uid(src_parent);
			final long dst_parent_uid = get_uid(dst_parent);
			final int src_host_idx = DFSClient.map_uid_to_host_idx(src_parent_uid);
			final int dst_host_idx = DFSClient.map_uid_to_host_idx(dst_parent_uid);
			//DFSClient.LOG.info("[dbg] rename.file: src_host_idx => " + src_host_idx);
			//DFSClient.LOG.info("[dbg] rename.file: dst_host_idx => " + dst_host_idx);
			
			if (src_host_idx == dst_host_idx) {
				//DFSClient.LOG.info("[dbg] rename.file: same host, no distributed transaction: host -->" + src_host_idx);
				return namenode_array[src_host_idx].rename(src, final_dst_path);
			}else {
				//DFSClient.LOG.info("[dbg] rename.file: starting distributed transaction");
			}
			
			 
			//use the src host as the coordinator
			String transaction_id = this.generate_uuid();
			final int coordinator_idx = src_host_idx;
			{//step 1: assign the coordinator
				DataOutputBuffer outputBuffer = new DataOutputBuffer();
	 			outputBuffer.writeBytes(transaction_id + DFSConfigKeys.new_line);
				outputBuffer.writeInt(2); // cohorts = parent + child;
				outputBuffer.writeInt(src_host_idx);
				outputBuffer.writeInt(dst_host_idx);
				//this.mina_rpc(coordinator_idx, outputBuffer.getData());
				
				DFSClient.pb_non_blocking_socket_rpc(coordinator_idx, 
						FSOperations.COORDINATOR.ordinal(), 
						outputBuffer.getData());
				outputBuffer.close();
				//DFSClient.LOG.info("[dbg] DFSClient.rename_file.assign_coordinator: [done] ");

			}
		
			
			// src cohort
			int [] target_host_array = new int[2];
			target_host_array[0] = src_host_idx;
			target_host_array[1] = dst_host_idx;
			long [] opcode = new long[2];
			opcode[0] = FSOperations.RENAME_FILE_DUO_SRC.ordinal();
			opcode[1] = FSOperations.RENAME_FILE_DUO_DST.ordinal();
			
			DataOutputBuffer[] msgs = new DataOutputBuffer[2];
			DataOutputBuffer src_dob = new DataOutputBuffer();
			DataOutputBuffer dst_dob = new DataOutputBuffer();

			src_dob.writeBytes(transaction_id + DFSConfigKeys.new_line);
			src_dob.writeInt(coordinator_idx); // coordinator idx
			src_dob.writeBytes(src + DFSConfigKeys.new_line);
			src_dob.writeBytes(final_dst_path + DFSConfigKeys.new_line);
			src_dob.writeInt(dst_host_idx); 
	 
			
			//child_outputBuffer.writeInt(FSOperations.MKDIR_DUO_CHILD.ordinal());
			dst_dob.writeBytes(transaction_id + DFSConfigKeys.new_line);
			dst_dob.writeInt(coordinator_idx); // coordinator idx
			dst_dob.writeBytes(src + DFSConfigKeys.new_line);
			dst_dob.writeBytes(final_dst_path + DFSConfigKeys.new_line);
			msgs[0] = src_dob;
			msgs[1] = dst_dob;
		 		
			Object[] results = this.parallel_pb_socket_rpc(target_host_array, opcode, msgs);
			src_dob.close();
			dst_dob.close();
			 
			DataInputBuffer src_result_dib = new DataInputBuffer();
			ByteString src_result_bs = (ByteString)results[0];
			src_result_dib.reset(src_result_bs.toByteArray(), src_result_bs.toByteArray().length);
			
			DataInputBuffer dst_result_dib = new DataInputBuffer();
			ByteString dst_result_bs = (ByteString)results[1];
			dst_result_dib.reset(dst_result_bs.toByteArray(), dst_result_bs.toByteArray().length);
			boolean src_result = src_result_dib.readBoolean();
			boolean dst_result = dst_result_dib.readBoolean(); 
			
			src_result_dib.close();
			dst_result_dib.close();

			return src_result && dst_result ;			 
			
			
		}
		 
	}

	/**
	 * Move blocks from src to trg and delete src See
	 * {@link ClientProtocol#concat(String, String [])}.
	 */
	public void concat(String trg, String[] srcs) throws IOException, UnresolvedLinkException {
		StringBuilder sb = new StringBuilder();
		for (String src: srcs) {
			sb.append(src);
			sb.append(":");
		}
		sb.substring(0, sb.length()-1);		
 		checkOpen();
		try {
			Path parentPath = (new Path(trg)).getParent();
			long parent_uid = get_uid(parentPath.toString());
			if (parent_uid < 0) {
				throw new FileNotFoundException("Parent doesn't exist for target: " + trg);				
			}
			int parent_host_idx = DFSClient.map_uid_to_host_idx(parent_uid);
			namenode_array[parent_host_idx].concat(trg, srcs);
			//getNN(trg).concat(trg, srcs);

    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class,
                                     UnresolvedPathException.class);
    }
  }
  /**
   * Rename file or directory.
   * See {@link ClientProtocol#rename(String, String, Options.Rename...)}
   */
  public void rename(String src, String dst, Options.Rename... options) 
      throws IOException, UnresolvedLinkException {
    checkOpen();
    try {
		this.rename(src, dst);
		// redirect this operation to dt-protected rename;
 		// getNN(src).rename(src, dst, options);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class,
                                     UnresolvedPathException.class);
    }
  }
  /**
   * Delete file or directory.
   * See {@link ClientProtocol#delete(String)}. 
   */
  @Deprecated
  public boolean delete(String src) 
      throws IOException, UnresolvedLinkException {
    checkOpen();
    return this.delete(src, true);
  }

  /**
   * delete file or directory.
   * delete contents of the directory if non empty and recursive 
   * set to true
   */
  public boolean delete(String src, boolean recursive) 
      throws IOException, UnresolvedLinkException {
    checkOpen();
    DFSClient.LOG.info("[dbg] dfsclient.delete target: " + src ); 
		boolean result = true;
		try {
			long uid = get_uid(src);
			if (uid == -1) {
				// this is a file
				DFSClient.LOG.info("[dbg] delete file branch");
				String parent_path = ((new Path(src))).getParent().toString();
				long parent_uid = get_uid(parent_path);
				if (parent_uid == -1) {
					// parent of target not exist, empty object, return; 
					return true;
				}
				int parent_host_idx = DFSClient.map_uid_to_host_idx(parent_uid);
				return namenode_array[parent_host_idx].delete(src);
				
			}else {
				// this is a directory
				DFSClient.LOG.info("[dbg] delete directory branch");
				//step 2: assign coordinator
				final int coordinator_idx = 0;
				String transaction_id = this.generate_uuid();
				DataOutputBuffer outputBuffer = new DataOutputBuffer();
				//outputBuffer.writeInt(FSOperations.COORDINATOR.ordinal());
				outputBuffer.writeBytes(transaction_id + DFSConfigKeys.new_line);
				outputBuffer.writeInt(hosts.length); // cohorts = parent + child;
				for (int i = 0; i < hosts.length; i++) {
					outputBuffer.writeInt(i);
				}
				DFSClient.pb_non_blocking_socket_rpc(coordinator_idx, 
							FSOperations.COORDINATOR.ordinal(), 
							outputBuffer.getData());
				outputBuffer.close();
				//DFSClient.LOG.info("[dbg] DFSClient.delete.assign_coordinator: [done] ");
				
				//step 3: boardcasting operations
				//DFSClient.LOG.info("[dbg] DFSClient.delete.preparing parameters: " + hosts.length);
				final int[] target_host_array = new int[hosts.length];
				final long[] opcode = new long[hosts.length];
				DataOutputBuffer[] msgs = new DataOutputBuffer[hosts.length];
				for (int i = 0; i < target_host_array.length; i++) {
					target_host_array[i] = i;
					opcode[i] = FSOperations.DELETE_DIR.ordinal();
					msgs[i] = new DataOutputBuffer();
					msgs[i].writeBytes(transaction_id + DFSConfigKeys.new_line);
					msgs[i].writeInt(coordinator_idx);
					msgs[i].writeBytes(src + DFSConfigKeys.new_line);
					msgs[i].writeBoolean(recursive);
				}
				
				//DFSClient.LOG.info("[dbg] DFSClient.delete......[starting parallel rpc]");
				Object[] results = this.parallel_pb_socket_rpc(target_host_array, opcode, msgs);
				final DataInputBuffer dib = new DataInputBuffer();
				for (int i = 0; i < hosts.length; i++) {
					byte[] data = ((ByteString)results[i]).toByteArray();
 					dib.reset(data, data.length);
					result = result && dib.readBoolean();
				}
				dib.close();
				if (result) {
					//DFSClient.LOG.info("[dbg] DFSClient.delete: remove_uid......");
					//remove uid
	 				result = this.remove_uid(src);  
				}
				
 				return result;
				 
				
			}
			
			
			
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     UnresolvedPathException.class);
    }
  }
  
  /** Implemented using getFileInfo(src)
   */
  public boolean exists(String src) throws IOException {
    checkOpen();
    return getFileInfo(src) != null;
  }

  /**
   * Get a partial listing of the indicated directory
   *
   * Recommend to use HdfsFileStatus.EMPTY_NAME as startAfter
   * if the application wants to fetch a listing starting from
   * the first entry in the directory
   *
   * @param src the directory name
   * @param startAfter the name to start listing after encoded in java UTF8
   * @return a partial listing starting after startAfter
   */
  public DirectoryListing listPaths(String src,  byte[] startAfter) 
    throws IOException, UnresolvedLinkException {
    checkOpen();
    try {
			//compatible for tdw... 
			//src may be a file...
			long src_uid = get_uid(src);
			if (src_uid == -1) {				 
				String parent_src_path = (new Path(src)).getParent().toString();
				long parent_src_uid = get_uid(parent_src_path);				
				if (parent_src_uid == -1) {					
					return null;
				}
				src_uid = parent_src_uid;
			}  
			int host_idx = DFSClient.map_uid_to_host_idx(src_uid);
			return namenode_array[host_idx].getListing(src, startAfter);
  			//return getNN2(src).getListing(src, startAfter);
		} catch (RemoteException re) {
			throw re.unwrapRemoteException(AccessControlException.class, UnresolvedPathException.class);
		}
	}

	final static int gdt_server_idx = 0;
	static public long get_uid(String src) throws IOException {
		
		DataOutputBuffer outputBuffer = new DataOutputBuffer();
 		outputBuffer.writeBytes(src + DFSConfigKeys.new_line);
		//return (Long) this.mina_rpc(gdt_server_idx, outputBuffer.getData());
		DataInputBuffer dib = pb_non_blocking_socket_rpc(gdt_server_idx,
					FSOperations.GET_UID.ordinal(), 
					outputBuffer.getData());
		long result = dib.readLong();
		dib.close(); outputBuffer.close();
		return result; 
	}
	
	 public long create_uid(String src) throws IOException{
		DataOutputBuffer outputBuffer = new DataOutputBuffer();
		//outputBuffer.writeInt(FSOperations.CREATE_UID.ordinal());
		outputBuffer.writeBytes(src + DFSConfigKeys.new_line);
		
		
		DataInputBuffer dib = DFSClient.pb_non_blocking_socket_rpc(gdt_server_idx,
				FSOperations.CREATE_UID.ordinal(), 
				outputBuffer.getData());
		long result = dib.readLong();
		dib.close();outputBuffer.close();
		return result;
		
	}
	
	 public Boolean remove_uid(String src) throws IOException{
		 DataOutputBuffer outputBuffer = new DataOutputBuffer();
		 //outputBuffer.writeInt(FSOperations.DELETE_PREFIX_UID.ordinal());
		 outputBuffer.writeBytes(src + DFSConfigKeys.new_line);
		 //return (Boolean) this.mina_rpc(gdt_server_idx, outputBuffer.getData());
		 DataInputBuffer dib = DFSClient.pb_non_blocking_socket_rpc(gdt_server_idx,
					FSOperations.DELETE_PREFIX_UID.ordinal(), 
					outputBuffer.getData());
		 Boolean result = dib.readBoolean();
		 dib.close();outputBuffer.close();
		 return result;
	}
	
	public Boolean rename_uid(String src, String dst) throws IOException{
		DataOutputBuffer outputBuffer = new DataOutputBuffer();
		//outputBuffer.writeInt(FSOperations.RENAME_UID.ordinal());
		outputBuffer.writeBytes(src + DFSConfigKeys.new_line);
		outputBuffer.writeBytes(dst + DFSConfigKeys.new_line);
		//return (Boolean) this.mina_rpc(gdt_server_idx, outputBuffer.getData());
		DataInputBuffer dib = DFSClient.pb_non_blocking_socket_rpc(gdt_server_idx,
				FSOperations.RENAME_UID.ordinal(), 
				outputBuffer.getData());
		Boolean result = dib.readBoolean();
		dib.close();outputBuffer.close();
		return result;
		
	}
	
	public HdfsFileStatus getFileInfo(String src) throws IOException, UnresolvedLinkException {
		checkOpen();
		DFSClient.LOG.info("[dbg] DFSClient.getFileInfo: " + src);
		try {
			long src_uid = get_uid(src);
			if (src_uid == -1) {
 				String parent_src_path = (new Path(src)).getParent().toString();
				long parent_src_uid = get_uid(parent_src_path);				 
				if (parent_src_uid == -1) {
 					return null;
				}
				src_uid = parent_src_uid;

			} 
			int host_idx = DFSClient.map_uid_to_host_idx(src_uid);			
			HdfsFileStatus result = namenode_array[host_idx].getFileInfo(src);
			return result;
			
			/*
			DataOutputBuffer outputBuffer = new DataOutputBuffer();
			//outputBuffer.writeInt(DFSConfigKeys.FSOperations.GET_FILEINFO.ordinal());
			outputBuffer.writeBytes(src + DFSConfigKeys.new_line);
			DataInputBuffer dib = pb_non_blocking_socket_rpc(host_idx, 
					FSOperations.GET_FILEINFO.ordinal(), 
					outputBuffer.getData());
			operation_tracer.println("[dbg] returned data buffer length: " + dib.getLength());
			//LOG.info("[dbg] returned byte size: " + dib.getLength());
			if (dib.getLength() == 0) {
				operation_tracer.println("[dbg] return null file info to upper call");
				//DFSClient.LOG.info("[dbg] DFSClient.getFileInfo --> returns null" );
				return null;
			} 
			
			HdfsFileStatus hfs = new HdfsFileStatus();
			hfs.readFields(dib); 
			//LOG.info("[dbg] DFSClient.getFileInfo --> returned HdfsFileStatus: " + hfs );
			operation_tracer.flush();
 			return hfs;*/ 
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     UnresolvedPathException.class);

     } catch (Exception e) {
			LOG.error(StringUtils.stringifyException(e));
 			throw new IOException(e);
		}
	}

  /**
   * Get the file info for a specific file or directory. If src
   * refers to a symlink then the FileStatus of the link is returned.
   * @param src path to a file or directory.
   * @throws IOException
   * @throws UnresolvedLinkException if the path contains symlinks
   * @return FileStatus describing src.
   */
  public HdfsFileStatus getFileLinkInfo(String src) 
      throws IOException, UnresolvedLinkException {
    checkOpen();
    try {
						
			long parent_dir_uid = get_uid((new Path(src)).getParent().toString());
			if (parent_dir_uid == -1) {
				return null;
			}
			int parent_host_idx = DFSClient.map_uid_to_host_idx(parent_dir_uid);
			return namenode_array[parent_host_idx].getFileLinkInfo(src);
			/*
			 *  ClientProtocol dirServer = getNN(src);
			HdfsFileStatus result = dirServer.getFileLinkInfo(src);
			*/
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     UnresolvedPathException.class);
     }
   }
  /**
   * Get the checksum of a file.
   * @param src The file path
   * @return The checksum 
   * @see DistributedFileSystem#getFileChecksum(Path)
   */
  public MD5MD5CRC32FileChecksum getFileChecksum(String src) throws IOException {
    checkOpen();
    return getFileChecksum(src, namenode, socketFactory, socketTimeout);    
  }

  /**
   * Get the checksum of a file.
   * @param src The file path
   * @return The checksum 
   */
  public static MD5MD5CRC32FileChecksum getFileChecksum(String src,
      ClientProtocol namenode, SocketFactory socketFactory, int socketTimeout
      ) throws IOException {
    //get all block locations
    List<LocatedBlock> locatedblocks
        = callGetBlockLocations(namenode, src, 0, Long.MAX_VALUE).getLocatedBlocks();
    final DataOutputBuffer md5out = new DataOutputBuffer();
    int bytesPerCRC = 0;
    long crcPerBlock = 0;
    boolean refetchBlocks = false;
    int lastRetriedIndex = -1;

    //get block checksum for each block
    for(int i = 0; i < locatedblocks.size(); i++) {
      if (refetchBlocks) {  // refetch to get fresh tokens
        locatedblocks = callGetBlockLocations(namenode, src, 0, Long.MAX_VALUE)
            .getLocatedBlocks();
        refetchBlocks = false;
      }
      LocatedBlock lb = locatedblocks.get(i);
      final Block block = lb.getBlock();
      final DatanodeInfo[] datanodes = lb.getLocations();
      
      //try each datanode location of the block
      final int timeout = 3000 * datanodes.length + socketTimeout;
      boolean done = false;
      for(int j = 0; !done && j < datanodes.length; j++) {
        Socket sock = null;
        DataOutputStream out = null;
        DataInputStream in = null;
        
        try {
          //connect to a datanode
          sock = socketFactory.createSocket();
          NetUtils.connect(sock,
              NetUtils.createSocketAddr(datanodes[j].getName()), timeout);
          sock.setSoTimeout(timeout);

          out = new DataOutputStream(
              new BufferedOutputStream(NetUtils.getOutputStream(sock), 
                                       DataNode.SMALL_BUFFER_SIZE));
          in = new DataInputStream(NetUtils.getInputStream(sock));

          if (LOG.isDebugEnabled()) {
            LOG.debug("write to " + datanodes[j].getName() + ": "
                + BLOCK_CHECKSUM + ", block=" + block);
          }
          // get block MD5
          DataTransferProtocol.Sender.opBlockChecksum(out, block.getBlockId(),
              block.getGenerationStamp(), lb.getAccessToken());

          final DataTransferProtocol.Status reply = DataTransferProtocol.Status.read(in);
          if (reply != SUCCESS) {
            if (reply == ERROR_ACCESS_TOKEN
                && i > lastRetriedIndex) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Got access token error in response to OP_BLOCK_CHECKSUM "
                    + "for file " + src + " for block " + block
                    + " from datanode " + datanodes[j].getName()
                    + ". Will retry the block once.");
              }
              lastRetriedIndex = i;
              done = true; // actually it's not done; but we'll retry
              i--; // repeat at i-th block
              refetchBlocks = true;
              break;
            } else {
              throw new IOException("Bad response " + reply + " for block "
                  + block + " from datanode " + datanodes[j].getName());
            }
          }

          //read byte-per-checksum
          final int bpc = in.readInt(); 
          if (i == 0) { //first block
            bytesPerCRC = bpc;
          }
          else if (bpc != bytesPerCRC) {
            throw new IOException("Byte-per-checksum not matched: bpc=" + bpc
                + " but bytesPerCRC=" + bytesPerCRC);
          }
          
          //read crc-per-block
          final long cpb = in.readLong();
          if (locatedblocks.size() > 1 && i == 0) {
            crcPerBlock = cpb;
          }

          //read md5
          final MD5Hash md5 = MD5Hash.read(in);
          md5.write(md5out);
          
          done = true;

          if (LOG.isDebugEnabled()) {
            if (i == 0) {
              LOG.debug("set bytesPerCRC=" + bytesPerCRC
                  + ", crcPerBlock=" + crcPerBlock);
            }
            LOG.debug("got reply from " + datanodes[j].getName()
                + ": md5=" + md5);
          }
        } catch (IOException ie) {
          LOG.warn("src=" + src + ", datanodes[" + j + "].getName()="
              + datanodes[j].getName(), ie);
        } finally {
          IOUtils.closeStream(in);
          IOUtils.closeStream(out);
          IOUtils.closeSocket(sock);        
        }
      }

      if (!done) {
        throw new IOException("Fail to get block MD5 for " + block);
      }
    }

    //compute file MD5
    final MD5Hash fileMD5 = MD5Hash.digest(md5out.getData()); 
    return new MD5MD5CRC32FileChecksum(bytesPerCRC, crcPerBlock, fileMD5);
  }

  /**
   * Set permissions to a file or directory.
   * @param src path name.
   * @param permission
   * @throws <code>FileNotFoundException</code> is file does not exist.
   * @throws UnresolvedLinkException if the path contains a symlink.
   */
  public void setPermission(String src, FsPermission permission)
      throws IOException, UnresolvedLinkException {
    checkOpen();
    try {
			
			Path parentPath = (new Path(src)).getParent();
			long parent_uid = get_uid(parentPath.toString());
			if (parent_uid <0 ) {
				return ;
			}
	int parent_host_idx = DFSClient.map_uid_to_host_idx(parent_uid);
	namenode_array[parent_host_idx].setPermission(src, permission);
 	//ClientProtocol dirServer = getNN(src);
	//dirServer.setPermission(src, permission);
  
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     UnresolvedPathException.class);
    }
  }

  /**
   * Set file or directory owner.
   * @param src path name.
   * @param username user id.
   * @param groupname user group.
   * @throws <code>FileNotFoundException</code> is file does not exist.
   * @throws UnresolvedLinkException if the path contains a symlink.
   */
  public void setOwner(String src, String username, String groupname)
      throws IOException, UnresolvedLinkException {
    checkOpen();
    try {
 			//ClientProtocol dirServer = getNN(src);
			//dirServer.setOwner(src, username, groupname);
 			
			Path parentPath = (new Path(src)).getParent();
			long parent_uid = get_uid(parentPath.toString());
			if (parent_uid <0) {
				return ;
			}
			int parent_host_idx = DFSClient.map_uid_to_host_idx(parent_uid);
			namenode_array[parent_host_idx].setOwner(src, username, groupname);
 
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     UnresolvedPathException.class);
    }
  }

  public FsStatus getDiskStatus() throws IOException {
		long rawNums[] = namenode_array[0].getStats();
 		// long rawNums[] = getNN("").getStats();
    return new FsStatus(rawNums[0], rawNums[1], rawNums[2]);
  }

  /**
   * Returns count of blocks with no good replicas left. Normally should be 
   * zero.
   * @throws IOException
   */ 
  public long getMissingBlocksCount() throws IOException {
		
		long missingBlockCount = 0;
		for (int i = 0; i < namenode_array.length; i++) {
			missingBlockCount += namenode_array[i].getStats()[ClientProtocol.GET_STATS_MISSING_BLOCKS_IDX];
		}

		return missingBlockCount;
		// return getNN("").getStats()[ClientProtocol.GET_STATS_MISSING_BLOCKS_IDX];
  }
  
  /**
   * Returns count of blocks with one of more replica missing.
   * @throws IOException
   */ 
  public long getUnderReplicatedBlocksCount() throws IOException {
		
		long underReplicatedBlocks = 0;
		for (int i = 0; i < namenode_array.length; i++) {
			underReplicatedBlocks += namenode_array[i].getStats()[ClientProtocol.GET_STATS_UNDER_REPLICATED_IDX];
		}

		return underReplicatedBlocks;
		// return getNN("").getStats()[ClientProtocol.GET_STATS_UNDER_REPLICATED_IDX];
  }
  
  /**
   * Returns count of blocks with at least one replica marked corrupt. 
   * @throws IOException
   */ 
  public long getCorruptBlocksCount() throws IOException {
	long corruptBlocksCount = 0;
	for (int i = 0; i < namenode_array.length; i++) {
		corruptBlocksCount += namenode_array[i].getStats()[ClientProtocol.GET_STATS_CORRUPT_BLOCKS_IDX];
	}

		return corruptBlocksCount;
		// return getNN("").getStats()[ClientProtocol.GET_STATS_CORRUPT_BLOCKS_IDX];
	}

  public DatanodeInfo[] datanodeReport(DatanodeReportType type)
  throws IOException {
		// FIXME: should issue requests to all the NNs
 		//return getNN("").getDatanodeReport(type);	

		DatanodeInfo[] result = null;
		for (int i = 0; i < namenode_array.length; i++) {
			result = namenode_array[i].getDatanodeReport(type);
			
		}
		return result;
		
  }
    
  /**
   * Enter, leave or get safe mode.
   * See {@link ClientProtocol#setSafeMode(FSConstants.SafeModeAction)} 
   * for more details.
   * 
   * @see ClientProtocol#setSafeMode(FSConstants.SafeModeAction)
   */
	public boolean setSafeMode(SafeModeAction action) throws IOException {
		boolean result = true;
		for (int i = 0; i < namenode_array.length; i++) {
			result = result && namenode_array[i].setSafeMode(action);
		}
		return result;
		//getNN("").setSafeMode(action);			

  }

  /**
   * Save namespace image.
   * See {@link ClientProtocol#saveNamespace()} 
   * for more details.
   * 
   * @see ClientProtocol#saveNamespace()
   */
	void saveNamespace() throws AccessControlException, IOException {
		try {
			for (int i = 0; i < namenode_array.length; i++) {
				namenode_array[i].saveNamespace();
			}
		} catch (RemoteException re) {
			throw re.unwrapRemoteException(AccessControlException.class);
		}
	}

  /**
   * enable/disable restore failed storage.
   * See {@link ClientProtocol#restoreFailedStorage(String arg)} 
   * for more details.
   * 
   */
  boolean restoreFailedStorage(String arg) throws AccessControlException {

		boolean result = true;
		for (int i = 0; i < namenode_array.length; i++) {
			result = result && namenode_array[i].restoreFailedStorage(arg);
		}

		// should issue requests to all the NNs
		// return getNN("").restoreFailedStorage(arg);
		return result;
  }

  /**
   * Refresh the hosts and exclude files.  (Rereads them.)
   * See {@link ClientProtocol#refreshNodes()} 
   * for more details.
   * 
   * @see ClientProtocol#refreshNodes()
   */
  public void refreshNodes() throws IOException {
		// should issue requests to all the NNs
		//getNN("").refreshNodes();
		for (int i = 0; i < namenode_array.length; i++) {
			namenode_array[i].refreshNodes();
		}
  }


  /**
   * Dumps DFS data structures into specified file.
   * See {@link ClientProtocol#metaSave(String)} 
   * for more details.
   * 
   * @see ClientProtocol#metaSave(String)
   */

  public void metaSave(String pathname) throws IOException {
		
		// should issue requests to all the NNs
		//getNN("").metaSave(pathname);
		for (int i = 0; i < namenode_array.length; i++) {
			namenode_array[i].metaSave(pathname);
		}
  }
    
  /**
   * @see ClientProtocol#finalizeUpgrade()
   */
  public void finalizeUpgrade() throws IOException {
		// should issue requests to all the NNs
		// getNN("").finalizeUpgrade();
		for (int i = 0; i < namenode_array.length; i++) {
			namenode_array[i].finalizeUpgrade();
		}
		
  }

  /**
   * @see ClientProtocol#distributedUpgradeProgress(FSConstants.UpgradeAction)
   */
  public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action
                                                        ) throws IOException {
		// FIXME: should issue requests to all the NNs, needs further testing....
		UpgradeStatusReport result = null;
		for (int i = 0; i < namenode_array.length; i++) {
			result = namenode_array[i].distributedUpgradeProgress(action);
		}
		return result;
		//return getNN("").distributedUpgradeProgress(action);
  }


	//add by wangyouwei 
	public String generate_uuid() {
		return UUID.randomUUID().toString();
 	}

	static public int map_uid_to_host_idx(long uid) {
		return (int)( uid % hosts.length);
	}
	//=======================================================

  /**
   */
  @Deprecated
  public boolean mkdirs(String src) throws IOException {
    return mkdirs(src, null, true);
  }

  /**
   * Create a directory (or hierarchy of directories) with the given
   * name and permission.
   *
   * @param src The path of the directory being created
   * @param permission The permission of the directory being created.
   * If permission == null, use {@link FsPermission#getDefault()}.
   * @param createParent create missing parent directory if true
   * @return True if the operation success.
   * @throws UnresolvedLinkException if the path contains a symlink.
   * @see ClientProtocol#mkdirs(String, FsPermission, boolean)
   */
  public boolean mkdirs(String src, FsPermission permission, boolean createParent)
      throws IOException, UnresolvedLinkException {
		// XXX: prepare procedure not matched for the parent and the child...
		checkOpen();
		DFSClient.LOG.info("[dbg] DFSClient.mkdirs: " + src); 
		
		if (permission == null) {
			permission = FsPermission.getDefault();
		}
		
		String parent_dir = (new Path(src)).getParent().toString();
		//DFSClient.LOG.info("[dbg] get_uid for parent_dir: " + parent_dir);
		long parent_uid = get_uid(parent_dir);
		//DFSClient.LOG.info("[dbg] parent_uid: " + parent_uid);
		if (parent_uid == -1) {
			//parent not exist
			if (createParent) {
				boolean mkdirs_parent_result = this.mkdirs(parent_dir, permission, createParent);
				if (!mkdirs_parent_result) {//failure
					return mkdirs_parent_result; // return false;
				}else {
					parent_uid = get_uid(parent_dir);
				}
			}else {
				//not to create the parent;
				return false;
			}
			
		}
		//DFSClient.LOG.info("[dbg] create child_uid@2014");
		long child_uid = create_uid(src);
		if (child_uid == -1) {
			//create the uid for child failed;
			//DFSClient.LOG.info("[dbg] create child_uid for src: " + src + " returns -1");
			return false;
		}
		
		int parent_host_idx = map_uid_to_host_idx(parent_uid) ;
		int child_host_idx = map_uid_to_host_idx(child_uid);
		if (parent_host_idx == child_host_idx) {
			
			//DFSClient.LOG.info("[dbg] parent_host_idx == child_host_idx: " + parent_host_idx);
			//DFSClient.LOG.info("[dbg] mkdir: no distributed transaction");
			return namenode_array[child_host_idx].mkdirs(src, permission, createParent);
			
		}else {
			
			//use the parent as the coordinator
			
			String transaction_id = this.generate_uuid();
			//DFSClient.LOG.info("[dbg] Starting distributed transaction: " + transaction_id);
			//DFSClient.LOG.info("[dbg] parent_host_idx: " + parent_host_idx);
			//DFSClient.LOG.info("[dbg] child_host_idx: " + child_host_idx);
			 
 			final int coordinator_idx = parent_host_idx;
			DataOutputBuffer outputBuffer = new DataOutputBuffer();
 			outputBuffer.writeBytes(transaction_id + DFSConfigKeys.new_line);
			outputBuffer.writeInt(2); // cohorts = parent + child;
			outputBuffer.writeInt(parent_host_idx);
			outputBuffer.writeInt(child_host_idx);
			//this.mina_rpc(coordinator_idx, outputBuffer.getData());
			
			DFSClient.pb_non_blocking_socket_rpc(coordinator_idx, 
					FSOperations.COORDINATOR.ordinal(), 
					outputBuffer.getData());
			outputBuffer.close();
			//DFSClient.LOG.info("[dbg] DFSClient.mkdirs.assign_coordinator: [done] ");
			
			final int[] target_host_array = new int[2];
			target_host_array[0] = parent_host_idx;
			target_host_array[1] = child_host_idx;
			final long[] opcode = new long[2];
			opcode[0] = FSOperations.MKDIR_DUO_PARENT.ordinal();
			opcode[1] = FSOperations.MKDIR_DUO_CHILD.ordinal();
 
			DataOutputBuffer[] msgs = new DataOutputBuffer[2];
			DataOutputBuffer parent_outputBuffer = new DataOutputBuffer();
			//parent_outputBuffer.writeInt(FSOperations.MKDIR_DUO_PARENT.ordinal());
			parent_outputBuffer.writeBytes(transaction_id + DFSConfigKeys.new_line);
			parent_outputBuffer.writeInt(coordinator_idx); // coordinator idx
			parent_outputBuffer.writeBytes(src + DFSConfigKeys.new_line);
			parent_outputBuffer.writeShort(permission.toShort());
			parent_outputBuffer.writeBoolean(createParent);
			msgs[0] = parent_outputBuffer;
			parent_outputBuffer.close();

			DataOutputBuffer child_outputBuffer = new DataOutputBuffer();
			//child_outputBuffer.writeInt(FSOperations.MKDIR_DUO_CHILD.ordinal());
			child_outputBuffer.writeBytes(transaction_id + DFSConfigKeys.new_line);
			child_outputBuffer.writeInt(coordinator_idx); // coordinator idx
			child_outputBuffer.writeBytes(src + DFSConfigKeys.new_line);
			child_outputBuffer.writeShort(permission.toShort());
			child_outputBuffer.writeBoolean(createParent);
			msgs[1] = child_outputBuffer;
 			child_outputBuffer.close();
 			
			Object[] results = this.parallel_pb_socket_rpc(target_host_array, opcode, msgs);
			//child_host_session.write(child_outputBuffer.getData());
			 

			DataInputBuffer parent_dib = new DataInputBuffer();
			ByteString parent_bytestring = (ByteString)results[0];
			parent_dib.reset(parent_bytestring.toByteArray(), 
					parent_bytestring.toByteArray().length);
			
			DataInputBuffer child_dib = new DataInputBuffer();
			ByteString child_bytestring = (ByteString)results[1];
			child_dib.reset(child_bytestring.toByteArray(), 
					child_bytestring.toByteArray().length);
			boolean parent_result = parent_dib.readBoolean();
			boolean child_result = child_dib.readBoolean(); 
			parent_dib.close();
			child_dib.close(); 
			final boolean final_result = parent_result && child_result;
			//DFSClient.LOG.info("[dbg] DFSClient.mkdirs->parent_result: " + parent_result);
			//DFSClient.LOG.info("[dbg] DFSClient.mkdirs->child_result: " + child_result);
			//DFSClient.LOG.info("[dbg] DFSClient.mkdirs: " + src + " [returned]");
			return final_result ;	
		}
		 
		 
  }
  
  /**
   * Same {{@link #mkdirs(String, FsPermission, boolean)} except
   * that the permissions has already been masked against umask.
   * @throws UnresolvedLinkException if the path contains a symlink.
   */
  public boolean primitiveMkdir(String src, FsPermission absPermission)
    throws IOException, UnresolvedLinkException {
    checkOpen();
    if (absPermission == null) {
      absPermission = 
        FsPermission.getDefault().applyUMask(FsPermission.getUMask(conf));
    } 

    LOG.debug(src + ": masked=" + absPermission);
    try {
      return this.mkdirs(src, absPermission, true);
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class,
                                     UnresolvedPathException.class);
    }
  }

  ContentSummary getContentSummary(String src) throws IOException {
    try {
			//ContentSummary result = getNN(src).getContentSummary(src);
			Path parentPath = (new Path(src)).getParent();
			long parent_uid = get_uid(parentPath.toString());
			if (parent_uid == -1) {
				return null;
			}
			int parent_host_idx = DFSClient.map_uid_to_host_idx(parent_uid);
			return namenode_array[parent_host_idx].getContentSummary(src);
		 
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     UnresolvedPathException.class);
    }
  }

  /**
   * Sets or resets quotas for a directory.
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#setQuota(String, long, long)
   */
  void setQuota(String src, long namespaceQuota, long diskspaceQuota) 
      throws IOException, UnresolvedLinkException {
    // sanity check
    if ((namespaceQuota <= 0 && namespaceQuota != FSConstants.QUOTA_DONT_SET &&
         namespaceQuota != FSConstants.QUOTA_RESET) ||
        (diskspaceQuota <= 0 && diskspaceQuota != FSConstants.QUOTA_DONT_SET &&
         diskspaceQuota != FSConstants.QUOTA_RESET)) {
      throw new IllegalArgumentException("Invalid values for quota : " +
                                         namespaceQuota + " and " + 
                                         diskspaceQuota);
                                         
    }
    try {
			Path parentPath = (new Path(src)).getParent();
			long parent_uid = get_uid(parentPath.toString());
			if (parent_uid == -1) {
				return;
			}
			int parent_host_idx = DFSClient.map_uid_to_host_idx(parent_uid);
			namenode_array[parent_host_idx].setQuota(src, namespaceQuota, diskspaceQuota);
 			//getNN(src).setQuota(src, namespaceQuota, diskspaceQuota);
     } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     NSQuotaExceededException.class,
                                     DSQuotaExceededException.class,
                                     UnresolvedPathException.class);
    }
  }

  /**
   * set the modification and access time of a file
   * @throws FileNotFoundException if the path is not a file
   */
  public void setTimes(String src, long mtime, long atime) 
      throws IOException, UnresolvedLinkException {
    checkOpen();
    try {
 			//getNN(src).setTimes(src, mtime, atime);
			Path parentPath = (new Path(src)).getParent();
			long parent_uid = get_uid(parentPath.toString());
			if (parent_uid == -1) {
				return;
			}
			int parent_host_idx = DFSClient.map_uid_to_host_idx(parent_uid);
			namenode_array[parent_host_idx].setTimes(src, mtime, atime);
			
    } catch(RemoteException re) {
      throw re.unwrapRemoteException(AccessControlException.class,
                                     FileNotFoundException.class,
                                     UnresolvedPathException.class);
    }
  }

  boolean isLeaseCheckerStarted() {
    return leasechecker.daemon != null;
  }

  /** Lease management*/
  class LeaseChecker implements Runnable {
    /** A map from src -> DFSOutputStream of files that are currently being
     * written by this client.
     */
    private final SortedMap<String, OutputStream> pendingCreates
        = new TreeMap<String, OutputStream>();

    private Daemon daemon = null;
    
    synchronized void put(String src, OutputStream out) {
      if (clientRunning) {
        if (daemon == null) {
          daemon = new Daemon(this);
          daemon.start();
        }
        pendingCreates.put(src, out);
      }
    }
    
    synchronized void remove(String src) {
      pendingCreates.remove(src);
    }
    
    void interruptAndJoin() throws InterruptedException {
      Daemon daemonCopy = null;
      synchronized (this) {
        if (daemon != null) {
          daemon.interrupt();
          daemonCopy = daemon;
        }
      }
     
      if (daemonCopy != null) {
        LOG.debug("Wait for lease checker to terminate");
        daemonCopy.join();
      }
    }

    void close() {
      while (true) {
        String src;
        OutputStream out;
        synchronized (this) {
          if (pendingCreates.isEmpty()) {
            return;
          }
          src = pendingCreates.firstKey();
          out = pendingCreates.remove(src);
        }
        if (out != null) {
          try {
            out.close();
          } catch (IOException ie) {
            LOG.error("Exception closing file " + src+ " : " + ie, ie);
          }
        }
      }
    }

    /**
     * Abort all open files. Release resources held. Ignore all errors.
     */
    synchronized void abort() {
      clientRunning = false;
      while (!pendingCreates.isEmpty()) {
        String src = pendingCreates.firstKey();
        DFSOutputStream out = (DFSOutputStream)pendingCreates.remove(src);
        if (out != null) {
          try {
            out.abort();
          } catch (IOException ie) {
            LOG.error("Exception aborting file " + src+ ": ", ie);
          }
        }
      }
      RPC.stopProxy(rpcNamenode); // close connections to the namenode
    }

    private void renew() throws IOException {
      synchronized(this) {
        if (pendingCreates.isEmpty()) {
          return;
        }
      }

	//add by wangyouwei
	//getNN("").renewLease(clientName);
	for (int i = 0; i < namenode_array.length; i++) {
		namenode_array[i].renewLease(clientName);
	}
	//================================================================
    }

    /**
     * Periodically check in with the namenode and renew all the leases
     * when the lease period is half over.
     */
    public void run() {
      long lastRenewed = 0;
      int renewal = (int)(LEASE_SOFTLIMIT_PERIOD / 2);
      if (hdfsTimeout > 0) {
        renewal = Math.min(renewal, hdfsTimeout/2);
      }
      while (clientRunning && !Thread.interrupted()) {
        if (System.currentTimeMillis() - lastRenewed > renewal) {
          try {
            renew();
            lastRenewed = System.currentTimeMillis();
          } catch (SocketTimeoutException ie) {
            LOG.warn("Problem renewing lease for " + clientName +
                     " for a period of " + (hdfsTimeout/1000) +
                     " seconds. Shutting down HDFS client...", ie);
            abort();
            break;
          } catch (IOException ie) {
            LOG.warn("Problem renewing lease for " + clientName +
                     " for a period of " + (hdfsTimeout/1000) +
                     " seconds. Will retry shortly...", ie);
          }
        }

        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(this + " is interrupted.", ie);
          }
          return;
        }
      }
    }

    /** {@inheritDoc} */
    public String toString() {
      String s = getClass().getSimpleName();
      if (LOG.isTraceEnabled()) {
        return s + "@" + DFSClient.this + ": "
               + StringUtils.stringifyException(new Throwable("for testing"));
      }
      return s;
    }
  }

  /**
   * The Hdfs implementation of {@link FSDataInputStream}
   */
  @InterfaceAudience.Private
  public static class DFSDataInputStream extends FSDataInputStream {
    public DFSDataInputStream(DFSInputStream in)
      throws IOException {
      super(in);
    }
      
    /**
     * Returns the datanode from which the stream is currently reading.
     */
    public DatanodeInfo getCurrentDatanode() {
      return ((DFSInputStream)in).getCurrentDatanode();
    }
      
    /**
     * Returns the block containing the target position. 
     */
    public Block getCurrentBlock() {
      return ((DFSInputStream)in).getCurrentBlock();
    }

    /**
     * Return collection of blocks that has already been located.
     */
    synchronized List<LocatedBlock> getAllBlocks() throws IOException {
      return ((DFSInputStream)in).getAllBlocks();
    }

    /**
     * @return The visible length of the file.
     */
    public long getVisibleLength() throws IOException {
      return ((DFSInputStream)in).getFileLength();
    }
  }

  void reportChecksumFailure(String file, Block blk, DatanodeInfo dn) {
    DatanodeInfo [] dnArr = { dn };
    LocatedBlock [] lblocks = { new LocatedBlock(blk, dnArr) };
    reportChecksumFailure(file, lblocks);
  }
    
  // just reports checksum failure and ignores any exception during the report.
  void reportChecksumFailure(String file, LocatedBlock lblocks[]) {
    try {
      reportBadBlocks(lblocks);
    } catch (IOException ie) {
      LOG.info("Found corruption while reading " + file 
               + ".  Error repairing corrupt blocks.  Bad blocks remain. " 
               + StringUtils.stringifyException(ie));
    }
  }

  /** {@inheritDoc} */
  public String toString() {
    return getClass().getSimpleName() + "[clientName=" + clientName
        + ", ugi=" + ugi + "]"; 
  }


	//add by dr.who	
	public void dumpNamespace() throws IOException {
		for (int i = 0; i < namenode_array.length; i++) {
		//namenode_array[i].dumpNamespace();
		}

	}
 
	//===================================================================================
	//google protocol buffer
	public Object[] parallel_pb_socket_rpc(int[] target_host_array, long[] opcode, DataOutputBuffer[] msgs) throws IOException{
		// Create a thread pool
		
		final ReentrantLock lock = new ReentrantLock();
		final Condition sync_condition = lock.newCondition();
		final boolean[] rpc_done = new boolean[target_host_array.length];
		for (int i = 0; i < rpc_done.length; i++) {
			rpc_done[i] = false;
		}
		final Object[] returned_objects = new Object[target_host_array.length];
		//final RpcController[] controllers = new SocketRpcController[target_host_array.length];
		//DFSClient.LOG.info("[dbg] parallel_pb_socket_rpc.port: " + this.pb_socket_rpc_port);
		for (int i = 0; i < target_host_array.length; i++) { 
			//controllers[i] = new SocketRpcController();
			//RpcConnectionFactory connectionFactory = SocketRpcConnectionFactories
			//	    .createRpcConnectionFactory(hosts[target_host_array[i]], this.pb_socket_rpc_port);
			//RpcChannel channel = RpcChannels.newRpcChannel(connectionFactory, thread_pool);
			//SendingData.process_data_service service = SendingData.process_data_service.newStub(channel);
			//controllers[i] = new SocketRpcController();
			
			ByteString data = ByteString.copyFrom(msgs[i].getData());
			process_data_request_proto request = process_data_request_proto.newBuilder()
					.setOpCode(opcode[i]).setData(data).build();
			final int current_idx = i;
			RpcCallback<process_data_response_proto> callback = new RpcCallback<process_data_response_proto>() {
				public void run(process_data_response_proto response) {
					// wake the main thread!
					try {
						lock.lock();
						returned_objects[current_idx] = response.getResult();
						synchronized (rpc_done) {
							rpc_done[current_idx] = true;
						}
						sync_condition.signalAll();
						
					}finally{
						lock.unlock();
					}
					
				}
			};
			 
			//service.processData(controllers[i], request, callback);
 			//services[i].processData(controllers[i], request, callback);
			final int target_idx = target_host_array[i];
 			non_blocking_services[target_idx].processData(controller, request, callback);
		}
		 
		try {
			do {
				lock.lock();
				boolean all_done = true;
				for (int i = 0; i < rpc_done.length; i++) {
					if (!rpc_done[i]) {
						all_done = false;
						
						if (NameNode.enable_failure_handling) {
							sync_condition.await(3000, TimeUnit.MILLISECONDS);
							synchronized (rpc_done) {
								if (rpc_done[i]) {
									//do nothing
								}else {
									throw new IOException("timeout when calling parallel_pb_socket_rpc@DFSClient");
								}
							}
							
						}else {
							sync_condition.await();
						}
						
 						break;
					}
				}
				if (all_done) {
					break;
				}
			} while (true);

		} catch (Exception e) {
			//DFSClient.LOG.info(StringUtils.stringifyException(e));
			throw new IOException(e);
		} finally {
			lock.unlock();
		}
		
		// Check success
		/*for (int i = 0; i < controllers.length; i++) {
			if (controllers[i].failed()) {
				//LOG.info(String.format("Rpc failed: %s", controllers[i].errorText()));
			}
		}*/
		
		if (controller.failed()) {
			LOG.info(String.format("Rpc failed: %s", controller.errorText()));
		}
		return returned_objects; //byte_strings
	}
	
	
	static public DataInputBuffer pb_non_blocking_socket_rpc(int target_host_idx, long op_code, byte[] msg) {
		// Create channel
		try {
			final ReentrantLock lock = new ReentrantLock();
			final Condition sync_condition = lock.newCondition();
			final int zero_idx = 0;
			final ByteString[] response_ref = new ByteString[1];
			final boolean[] rpc_done = new boolean[1];
			
			response_ref[zero_idx] = null;
			rpc_done[zero_idx] = false;
  			ByteString data = ByteString.copyFrom(msg);
			 
			RpcCallback<process_data_response_proto> callback = new RpcCallback<process_data_response_proto>() {
				public void run(process_data_response_proto response) {
					try {
						lock.lock();
						response_ref[zero_idx] = response.getResult();
						synchronized (rpc_done) {
							rpc_done[zero_idx] = true;
						}
  						sync_condition.signalAll();
						
					}finally{
						lock.unlock();
					}
					
				}
			};
			 
			 
 			non_blocking_services[target_host_idx].processData(controller, 
					process_data_request_proto.newBuilder().setOpCode(op_code).setData(data).build(),
					callback); 
			
			 try {
				lock.lock();
				if (!rpc_done[zero_idx]) {
					if (NameNode.enable_failure_handling) {
						sync_condition.await(3000, TimeUnit.MILLISECONDS);
						synchronized (rpc_done) {
							if (rpc_done[zero_idx]) {
								//do nothing
							}else {
								throw new IOException("timeout when calling pb_non_blocking_socket_rpc@DFSClient");
							}
						}
						
					}else {
						sync_condition.await();
					}
					
				}
				
			} catch (Exception e) {
				//DFSClient.LOG.info(StringUtils.stringifyException(e));
				throw new IOException(e);
			} finally {
				lock.unlock();
			} 
  			
			
			//if (controller.failed()) {
 			if (controller.failed()) {
 				//DFSClient.LOG.info("[dbg] pb-rpc-controller fatal error: " + controller.errorText());
 				//System.err.println(String.format("Rpc failed %s : %s", rpcController.errorReason(),
 				//    rpcController.errorText()));
				return null;
			}else {
				ByteString result = response_ref[zero_idx];
				DataInputBuffer dib = new DataInputBuffer();
				dib.reset(result.toByteArray(), result.toByteArray().length);
				////LOG.info("[dbg] msg returned by pb-socket-rpc: " + dib.readLine()); 
				return dib;
				//processing the result
			}
		} catch (Exception e) {
 			 DFSClient.LOG.error(StringUtils.stringifyException(e));
 		}
		
 		return null;
		
		
 	} 
	
	
}



