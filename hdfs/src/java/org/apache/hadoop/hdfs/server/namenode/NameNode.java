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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.DFSConfigKeys.FSOperations;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.security.ExportedAccessKeys;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.PoolClient;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.CompleteFileStatus;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.CloverProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.NodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.SendingData;
import org.apache.hadoop.hdfs.SendingData.process_data_request_proto;
import org.apache.hadoop.hdfs.SendingData.process_data_response_proto;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.RefreshUserToGroupMappingsProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;

import org.apache.hadoop.util.ServicePlugin;
import org.apache.hadoop.util.StringUtils;
  
//google protobuf 
import com.google.protobuf.*;
import com.googlecode.protobuf.socketrpc.*;
import org.apache.hadoop.hdfs.SendingData.process_data_service.BlockingInterface;
  



/**********************************************************
 * NameNode serves as both directory namespace manager and
 * "inode table" for the Hadoop DFS.  There is a single NameNode
 * running in any DFS deployment.  (Well, except when there
 * is a second backup/failover NameNode.)
 *
 * The NameNode controls two critical tables:
 *   1)  filename->blocksequence (namespace)
 *   2)  block->machinelist ("inodes")
 *
 * The first table is stored on disk and is very precious.
 * The second table is rebuilt every time the NameNode comes
 * up.
 *
 * 'NameNode' refers to both this class as well as the 'NameNode server'.
 * The 'FSNamesystem' class actually performs most of the filesystem
 * management.  The majority of the 'NameNode' class itself is concerned
 * with exposing the IPC interface and the http server to the outside world,
 * plus some configuration management.
 *
 * NameNode implements the ClientProtocol interface, which allows
 * clients to ask for DFS services.  ClientProtocol is not
 * designed for direct use by authors of DFS client code.  End-users
 * should instead use the org.apache.nutch.hadoop.fs.FileSystem class.
 *
 * NameNode also implements the DatanodeProtocol interface, used by
 * DataNode programs that actually store DFS data blocks.  These
 * methods are invoked repeatedly and automatically by all the
 * DataNodes in a DFS deployment.
 *
 * NameNode also implements the NamenodeProtocol interface, used by
 * secondary namenodes or rebalancing processes to get partial namenode's
 * state, for example partial blocksMap etc.
 **********************************************************/
@InterfaceAudience.Private
@SuppressWarnings("rawtypes")
public class NameNode implements NamenodeProtocols, FSConstants {
  static{
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
  }
  
  public long getProtocolVersion(String protocol, 
                                 long clientVersion) throws IOException { 
    if (protocol.equals(ClientProtocol.class.getName())) {
      return ClientProtocol.versionID; 
    } else if (protocol.equals(DatanodeProtocol.class.getName())){
      return DatanodeProtocol.versionID;
    } else if (protocol.equals(CloverProtocol.class.getName())) {
	return CloverProtocol.versionID;
    } else if (protocol.equals(NamenodeProtocol.class.getName())){
      return NamenodeProtocol.versionID;
    } else if (protocol.equals(RefreshAuthorizationPolicyProtocol.class.getName())){
      return RefreshAuthorizationPolicyProtocol.versionID;
    } else if (protocol.equals(RefreshUserToGroupMappingsProtocol.class.getName())){
      return RefreshUserToGroupMappingsProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol to name node: " + protocol);
    }
  }
    
  public static final int DEFAULT_PORT = 8020;

  public static final Log LOG = LogFactory.getLog(NameNode.class.getName());
  public static final Log stateChangeLog = LogFactory.getLog("org.apache.hadoop.hdfs.StateChange");

	//add by jay
  public static PoolClient client = null;

  protected FSNamesystem namesystem; 
  protected NamenodeRole role;
  /** RPC server */
  protected Server server;
  /** RPC server address */
  protected InetSocketAddress rpcAddress = null;
  /** httpServer */
  protected HttpServer httpServer;
  /** HTTP server address */
  protected InetSocketAddress httpAddress = null;
  private Thread emptier;
  /** only used for testing purposes  */
  protected boolean stopRequested = false;
  /** Registration information of this name-node  */
  protected NamenodeRegistration nodeRegistration;
  /** Is service level authorization enabled? */
  private boolean serviceAuthEnabled = false;
  @SuppressWarnings("deprecation")
  /** Activated plug-ins. */
  private List<ServicePlugin> plugins;
  
  /** Format a new filesystem.  Destroys any filesystem that may already
   * exist at this location.  **/
  public static void format(Configuration conf) throws IOException {
    format(conf, false);
  }

	

	// fields of DNN
 	public static Collection<URI> dnnNames;
	public static int selfId = -1;

	public void setSelfId(Configuration conf) {
		ArrayList<URI> names = (ArrayList<URI>) dnnNames;

		for (int i = 0; i < names.size(); i++) {
			if (names.get(i).getAuthority().equals(FileSystem.getDefaultUri(conf).getAuthority())) {
				selfId = i;
				break;
			}
		}
	}

	public static Collection<URI> getDnnURI(Configuration conf) {
		Collection<String> dnnNames = conf.getStringCollection(DFSConfigKeys.DFS_DNN_LIST_KEY);  
		return Util.stringCollectionAsURIs(dnnNames);
	}

	
	public Collection<CloverProtocol> getDnn(Collection<URI> uri, Configuration conf) throws IOException {
		Collection<CloverProtocol> dnns = new ArrayList<CloverProtocol>();
		int i = 0;
		for (URI nn : uri) {
			if (i == selfId) {
				LOG.info("Connecting to NN: " + nn + " in LOCAL model");
				dnns.add((CloverProtocol) RPC.waitForProxy(CloverProtocol.class, 
						CloverProtocol.versionID, NameNode.getAddress(conf), conf));
			} else {
				LOG.info("Connecting to NN: " + nn + " in REMOTE model");
				dnns.add((CloverProtocol) RPC.waitForProxy(CloverProtocol.class, 
						CloverProtocol.versionID, NameNode.getAddress(nn.getAuthority()), conf));
			}
			i++;
		}
		return dnns;
	}

 
	static NameNodeMetrics myMetrics;

  /** Return the {@link FSNamesystem} object.
   * @return {@link FSNamesystem} object.
   */
  FSNamesystem getNamesystem() {
    return namesystem;
  }

  static void initMetrics(Configuration conf, NamenodeRole role) {
    myMetrics = new NameNodeMetrics(conf, role);
  }

  public static NameNodeMetrics getNameNodeMetrics() {
    return myMetrics;
  }
  
  public static InetSocketAddress getAddress(String address) {
    return NetUtils.createSocketAddr(address, DEFAULT_PORT);
  }

  public static InetSocketAddress getAddress(Configuration conf) {
    URI filesystemURI = FileSystem.getDefaultUri(conf);
    String authority = filesystemURI.getAuthority();
    if (authority == null) {
      throw new IllegalArgumentException(String.format(
          "Invalid URI for NameNode address (check %s): %s has no authority.",
          FileSystem.FS_DEFAULT_NAME_KEY, filesystemURI.toString()));
    }
    if (!FSConstants.HDFS_URI_SCHEME.equalsIgnoreCase(
        filesystemURI.getScheme())) {
      throw new IllegalArgumentException(String.format(
          "Invalid URI for NameNode address (check %s): %s is not of scheme '%s'.",
          FileSystem.FS_DEFAULT_NAME_KEY, filesystemURI.toString(),
          FSConstants.HDFS_URI_SCHEME));
    }
    return getAddress(authority);
  }

  public static URI getUri(InetSocketAddress namenode) {
    int port = namenode.getPort();
    String portString = port == DEFAULT_PORT ? "" : (":"+port);
    return URI.create(FSConstants.HDFS_URI_SCHEME + "://" 
        + namenode.getHostName()+portString);
  }

  /**
   * Compose a "host:port" string from the address.
   */
  public static String getHostPortString(InetSocketAddress addr) {
    return addr.getHostName() + ":" + addr.getPort();
  }

  //
  // Common NameNode methods implementation for the active name-node role.
  //
  public NamenodeRole getRole() {
    return role;
  }

  boolean isRole(NamenodeRole that) {
    return role.equals(that);
  }

  protected InetSocketAddress getRpcServerAddress(Configuration conf) throws IOException {
    return getAddress(conf);
  }

  protected void setRpcServerAddress(Configuration conf) {
    FileSystem.setDefaultUri(conf, getUri(rpcAddress));
  }

  protected InetSocketAddress getHttpServerAddress(Configuration conf) {
    return  NetUtils.createSocketAddr(
        conf.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:50070"));
  }

  protected void setHttpServerAddress(Configuration conf){
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, getHostPortString(httpAddress));
  }

  protected void loadNamesystem(Configuration conf) throws IOException {
    this.namesystem = new FSNamesystem(conf);
  }

  NamenodeRegistration getRegistration() {
    return nodeRegistration;
  }

  NamenodeRegistration setRegistration() {
    nodeRegistration = new NamenodeRegistration(
        getHostPortString(rpcAddress),
        getHostPortString(httpAddress),
        getFSImage(), getRole(), getFSImage().getCheckpointTime());
    return nodeRegistration;
  }

	/**
	 * Initialize name-node.
	 * 
	 * @param conf the configuration
	 */
	
	static public final boolean enable_concurrent_control = false;
	static public final boolean enable_failure_handling = false;
	static public final int unified_timeout = 3000; // in milliseconds
		
	static String[] hosts = null;
	RpcController controller = null;
	RpcChannel[] non_blocking_channels = null;
	SendingData.process_data_service[] non_blocking_services = null;
	public static int pb_socket_rpc_port = -1;
	static final int CONNECT_TIMEOUT = 1000;
	
	@SuppressWarnings("deprecation")
	protected void initialize(Configuration conf) throws IOException {
		LOG.info("[inf] initializing NameNode->self_id: " + NameNode.selfId);
		InetSocketAddress socAddr = getRpcServerAddress(conf);
		int handlerCount = conf.getInt("dfs.namenode.handler.count", 10);

		// set service-level authorization security policy
		if (serviceAuthEnabled = conf.getBoolean(ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG, false)) {
			ServiceAuthorizationManager.refresh(conf, new HDFSPolicyProvider());
		}
		LOG.info("[inf] namenode initMetrics");
		NameNode.initMetrics(conf, this.getRole());
		loadNamesystem(conf);
		// create rpc server
		this.server = RPC.getServer(NamenodeProtocols.class, this, socAddr.getHostName(), socAddr.getPort(), handlerCount, false, conf, namesystem.getDelegationTokenSecretManager());
		// The rpc-server port can be ephemeral... ensure we have the correct
		// info
		this.rpcAddress = this.server.getListenerAddress();
		setRpcServerAddress(conf);
 		activate(conf);
		LOG.info(getRole() + " up at: " + rpcAddress);
		  
		//============================================================================
 		try {
 			//NameNode.LOG.info("[dbg] parsing namenode hostname: ");
			String dnn_lists = conf.get(DFSConfigKeys.DFS_DNN_LIST_KEY);
			String[] socketPortUnits = dnn_lists.split(",");
			hosts = new String[socketPortUnits.length];
			int cursor = 0;
			for (String _socketportUnit : socketPortUnits) {
				URI uri = new URI(_socketportUnit);
				hosts[cursor] = uri.getHost();
 				cursor++;

			}
		} catch (Exception e) {
			LOG.error(StringUtils.stringifyException(e));
			throw new IOException(e);
		}
		
		try {
			NameNode.pb_socket_rpc_port = Integer.valueOf(conf.get(DFSConfigKeys.PROTOCOL_BUFFER_SOCKET_RPC_PORT)).intValue();
			LOG.info("Starting Namenode ProtocolBuffer SocketRPC port at: " + NameNode.pb_socket_rpc_port);
			final int threadPoolSize = 100;
			ServerRpcConnectionFactory rpcConnectionFactory = SocketRpcConnectionFactories
				    .createServerRpcConnectionFactory(pb_socket_rpc_port);
			RpcServer server = new RpcServer(rpcConnectionFactory, 
					Executors.newFixedThreadPool(threadPoolSize), true);
	  		//server.registerService(new process_data_protocol()); // For non-blocking impl
			process_data_service_impl namenode_pb_rpc_processor = new process_data_service_impl();
			namenode_pb_rpc_processor.set_namenode_ref(this);
			server.registerBlockingService(SendingData.process_data_service
					.newReflectiveBlockingService(
					namenode_pb_rpc_processor)); // For blocking impl
			//server.run();
			server.startServer();
			//LOG.info("[dbg] namenode.protocolbuffer.socket.rpc: server started at port: " + pb_socket_rpc_port);
			//LOG.info("[dbg] namenode.protocolbuffer.socket.rpc: initializing client:");
			
			non_blocking_channels = new RpcChannel[hosts.length];
			non_blocking_services = new SendingData.process_data_service[hosts.length];
			
			controller = new SocketRpcController();
			for (int i = 0; i < hosts.length; i++) {
				final String host = hosts[i];
				final int port = NameNode.pb_socket_rpc_port; //ports[target_host_array[i]];
				RpcConnectionFactory connectionFactory = 
						SocketRpcConnectionFactories.createRpcConnectionFactory(host, port);
				non_blocking_channels[i] = RpcChannels.newRpcChannel(connectionFactory, 
						Executors.newFixedThreadPool(threadPoolSize)); 
				non_blocking_services[i] = SendingData.process_data_service.newStub(non_blocking_channels[i]);
				//RpcChannels.newBlockingRpcChannel(connectionFactory);
				//non_blocking_services[i] = SendingData.process_data_service.newBlockingStub(non_blocking_channels[i]);				
			}
			LOG.info("Namenode ProtocolBuffer SocketRPC Client Ready...");
		} catch (Exception e) {
			LOG.error(StringUtils.stringifyException(e));
			throw new IOException(e);
		}		
		
	} 
	
 

  /**
   * Activate name-node servers and threads.
   */
  @SuppressWarnings("deprecation")
  void activate(Configuration conf) throws IOException {
    if ((isRole(NamenodeRole.ACTIVE))
        && (UserGroupInformation.isSecurityEnabled())) {
      namesystem.activateSecretManager();
    }
    namesystem.activate(conf);
    startHttpServer(conf);
    server.start();  //start RPC server
    startTrashEmptier(conf);
    
    plugins = conf.getInstances("dfs.namenode.plugins", ServicePlugin.class);
    for (ServicePlugin p: plugins) {
      try {
        p.start(this);
      } catch (Throwable t) {
        LOG.warn("ServicePlugin " + p + " could not be started", t);
      }
    }
  }

  private void startTrashEmptier(Configuration conf) throws IOException {
    long trashInterval = conf.getLong("fs.trash.interval", 0);
    if(trashInterval == 0)
      return;
    this.emptier = new Thread(new Trash(conf).getEmptier(), "Trash Emptier");
    this.emptier.setDaemon(true);
    this.emptier.start();
  }

  private void startHttpServer(Configuration conf) throws IOException {
    InetSocketAddress infoSocAddr = getHttpServerAddress(conf);
    String infoHost = infoSocAddr.getHostName();
    int infoPort = infoSocAddr.getPort();
    this.httpServer = new HttpServer("hdfs", infoHost, infoPort, 
        infoPort == 0, conf);
    if (conf.getBoolean("dfs.https.enable", false)) {
      boolean needClientAuth = conf.getBoolean(DFSConfigKeys.DFS_CLIENT_HTTPS_NEED_AUTH_KEY,
                                               DFSConfigKeys.DFS_CLIENT_HTTPS_NEED_AUTH_DEFAULT);
      InetSocketAddress secInfoSocAddr = NetUtils.createSocketAddr(conf.get(
          DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY, infoHost + ":" + 0));
      Configuration sslConf = new HdfsConfiguration(false);
      sslConf.addResource(conf.get("dfs.https.server.keystore.resource",
          "ssl-server.xml"));
      this.httpServer.addSslListener(secInfoSocAddr, sslConf, needClientAuth);
      // assume same ssl port for all datanodes
      InetSocketAddress datanodeSslPort = NetUtils.createSocketAddr(conf.get(
          "dfs.datanode.https.address", infoHost + ":" + 50475));
      this.httpServer.setAttribute("datanode.https.port", datanodeSslPort
          .getPort());
    }
    this.httpServer.setAttribute("name.node", this);
    this.httpServer.setAttribute("name.node.address", getNameNodeAddress());
    this.httpServer.setAttribute("name.system.image", getFSImage());
    this.httpServer.setAttribute("name.conf", conf);
    this.httpServer.addInternalServlet("getDelegationToken", 
        DelegationTokenServlet.PATH_SPEC, DelegationTokenServlet.class);
    this.httpServer.addInternalServlet("fsck", "/fsck", FsckServlet.class);
    this.httpServer.addInternalServlet("getimage", "/getimage", GetImageServlet.class);
    this.httpServer.addInternalServlet("listPaths", "/listPaths/*", ListPathsServlet.class);
    this.httpServer.addInternalServlet("data", "/data/*", FileDataServlet.class);
    this.httpServer.addInternalServlet("checksum", "/fileChecksum/*",
        FileChecksumServlets.RedirectServlet.class);
    this.httpServer.addInternalServlet("contentSummary", "/contentSummary/*",
        ContentSummaryServlet.class);
    this.httpServer.start();

    // The web-server port can be ephemeral... ensure we have the correct info
    infoPort = this.httpServer.getPort();
    this.httpAddress = new InetSocketAddress(infoHost, infoPort);
    setHttpServerAddress(conf);
    LOG.info(getRole() + " Web-server up at: " + httpAddress);
  }

  /**
   * Start NameNode.
   * <p>
   * The name-node can be started with one of the following startup options:
   * <ul> 
   * <li>{@link StartupOption#REGULAR REGULAR} - normal name node startup</li>
   * <li>{@link StartupOption#FORMAT FORMAT} - format name node</li>
   * <li>{@link StartupOption#BACKUP BACKUP} - start backup node</li>
   * <li>{@link StartupOption#CHECKPOINT CHECKPOINT} - start checkpoint node</li>
   * <li>{@link StartupOption#UPGRADE UPGRADE} - start the cluster  
   * upgrade and create a snapshot of the current file system state</li> 
   * <li>{@link StartupOption#ROLLBACK ROLLBACK} - roll the  
   *            cluster back to the previous state</li>
   * <li>{@link StartupOption#FINALIZE FINALIZE} - finalize 
   *            previous upgrade</li>
   * <li>{@link StartupOption#IMPORT IMPORT} - import checkpoint</li>
   * </ul>
   * The option is passed via configuration field: 
   * <tt>dfs.namenode.startup</tt>
   * 
   * The conf will be modified to reflect the actual ports on which 
   * the NameNode is up and running if the user passes the port as
   * <code>zero</code> in the conf.
   * 
   * @param conf  confirguration
   * @throws IOException
   */
  public NameNode(Configuration conf) throws IOException {
    this(conf, NamenodeRole.ACTIVE);
  }

  @SuppressWarnings("deprecation")
  protected NameNode(Configuration conf, NamenodeRole role) 
      throws IOException { 
    UserGroupInformation.setConfiguration(conf);
    DFSUtil.login(conf, 
        DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY,
        DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY);

		// add by wangyouwei 
		cohort_set = new ConcurrentHashMap<String, DTCohort>();
		coordinator_set = new ConcurrentHashMap<String, DTCoordinator>();
 
		this.role = role;
		// init clover settings and arrays
		dnnNames = getDnnURI(conf);
		setSelfId(conf);
 		if (selfId == -1) {
 			LOG.error("illegal selfId, unable to identify current host, abort");
			throw new IOException();
		}else{
			NameNode.LOG.info("Current host selfid: " + selfId);
		}			
 		
		try {
			initialize(conf);
		} catch (IOException e) {
			LOG.error(StringUtils.stringifyException(e));
			this.stop();
			throw e;
		}
	}

	/**
   	 * Wait for service to finish.
     	 * (Normally, it runs forever.)
   	 */
  	@SuppressWarnings("deprecation")
	public void join() {
		try {
			this.server.join();
		} catch (InterruptedException ie) {
			LOG.error(StringUtils.stringifyException(ie));
		}
	}

  /**
   * Stop all NameNode threads and wait for all to finish.
   */
  @SuppressWarnings("deprecation")
  public void stop() {
    synchronized(this) {
      if (stopRequested)
        return;
      stopRequested = true;
    }
    if (plugins != null) {
      for (ServicePlugin p : plugins) {
        try {
          p.stop();
        } catch (Throwable t) {
          LOG.warn("ServicePlugin " + p + " could not be stopped", t);
        }
      }
    }
    try {
      if (httpServer != null) httpServer.stop();
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
    }
    if(namesystem != null) namesystem.close();
    if(emptier != null) emptier.interrupt();
    if(server != null) server.stop();
    if (myMetrics != null) {
      myMetrics.shutdown();
    }
    if (namesystem != null) {
      namesystem.shutdown();
    }
  }

  synchronized boolean isStopRequested() {
    return stopRequested;
  }

  /////////////////////////////////////////////////////
  // NamenodeProtocol
  /////////////////////////////////////////////////////
  @Override // NamenodeProtocol
  public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size)
  throws IOException {
    if(size <= 0) {
      throw new IllegalArgumentException(
        "Unexpected not positive size: "+size);
    }

    return namesystem.getBlocks(datanode, size); 
  }

  /** {@inheritDoc} */
  public ExportedAccessKeys getAccessKeys() throws IOException {
    return namesystem.getAccessKeys();
  }

  @Override // NamenodeProtocol
  public void errorReport(NamenodeRegistration registration,
                          int errorCode, 
                          String msg) throws IOException {
    verifyRequest(registration);
    LOG.info("Error report from " + registration + ": " + msg);
    if(errorCode == FATAL)
      namesystem.releaseBackupNode(registration);
  }

  @Override // NamenodeProtocol
  public NamenodeRegistration register(NamenodeRegistration registration)
  throws IOException {
    verifyVersion(registration.getVersion());
    namesystem.registerBackupNode(registration);
    return setRegistration();
  }

  @Override // NamenodeProtocol
  public NamenodeCommand startCheckpoint(NamenodeRegistration registration)
  throws IOException {
    verifyRequest(registration);
    if(!isRole(NamenodeRole.ACTIVE))
      throw new IOException("Only an ACTIVE node can invoke startCheckpoint.");
    return namesystem.startCheckpoint(registration, setRegistration());
  }

  @Override // NamenodeProtocol
  public void endCheckpoint(NamenodeRegistration registration,
                            CheckpointSignature sig) throws IOException {
    verifyRequest(registration);
    if(!isRole(NamenodeRole.ACTIVE))
      throw new IOException("Only an ACTIVE node can invoke endCheckpoint.");
    namesystem.endCheckpoint(registration, sig);
  }

  @Override // NamenodeProtocol
  public long journalSize(NamenodeRegistration registration)
  throws IOException {
    verifyRequest(registration);
    return namesystem.getEditLogSize();
  }

  /*
   * Active name-node cannot journal.
   */
  @Override // NamenodeProtocol
  public void journal(NamenodeRegistration registration,
                      int jAction,
                      int length,
                      byte[] args) throws IOException {
    throw new UnsupportedActionException("journal");
  }

  /////////////////////////////////////////////////////
  // ClientProtocol
  /////////////////////////////////////////////////////
  
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    return namesystem.getDelegationToken(renewer);
  }

  @Override
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws InvalidToken, IOException {
    return namesystem.renewDelegationToken(token);
  }

  @Override
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    namesystem.cancelDelegationToken(token);
  }
  
  /** {@inheritDoc} */
  public LocatedBlocks getBlockLocations(String src, 
                                          long offset, 
                                          long length) 
	      throws IOException {
		myMetrics.numGetBlockLocations.inc();
		return namesystem.getBlockLocations(getClientMachine(), src, offset, length);
	}

	public long getUID(String src) throws IOException {
		return namesystem.gdt.lookup(src);
	}

	

	public long createUID(String src) throws IOException {
		long id = namesystem.gdt.getID();
		boolean notexist = namesystem.gdt.set(src, id);
		if (!notexist) {
			// this src has been existed in GDT map
			return -1L;
		}
		synchronized (namesystem.dir.rootDir) {
			namesystem.getEditLog().logSetUID(src, id, namesystem.gdt.getCID());
			namesystem.getEditLog().logSync();
		}
		return id;
	}

	 

	public boolean removeUIDWithoutLog(String src) throws IOException {
		Long id = namesystem.gdt.removeAndReturn(src);
		if (id == null)
			return false;

		return true;
	}

	public boolean logRemoveUID(String src, long uid) throws IOException {
		synchronized (namesystem.dir.rootDir) {
			namesystem.getEditLog().logRemoveUID(src, uid);
			namesystem.getEditLog().logSync();
		}
		return true;
	}

	public boolean logRemoveUIDTransactionExt(String uuid, String src, long uid) throws IOException {
		synchronized (namesystem.dir.rootDir) {
			namesystem.getEditLog().logRemoveUIDTransactionExt(uuid, src, uid);
			namesystem.getEditLog().logSync();
		}
		return true;
	}

	public boolean removeUID(String src) throws IOException {
		Long id = namesystem.gdt.removeAndReturn(src);
		if (id == null)
			return false;
		synchronized (namesystem.dir.rootDir) {
			namesystem.getEditLog().logRemoveUID(src, id.longValue());
			namesystem.getEditLog().logSync();
		}

		return true;
	}
	
	public boolean delete_prefix_uid(String src) throws IOException {
		namesystem.gdt.delete_with_prefix(src);
		synchronized (namesystem.dir.rootDir) {
			namesystem.getEditLog().log_delete_prefix_uid(src);
			namesystem.getEditLog().logSync();
		}

		return true;
	}

	public boolean removeUIDwithoutLog(String src) throws IOException {
		Long id = namesystem.gdt.removeAndReturn(src);
		if (id == null)
			return false;

		return true;
	}

	public long removeUIDAndReturn(String src) throws IOException {
		Long id = namesystem.gdt.removeAndReturn(src);
		if (id == null)
			return -1;

		synchronized (namesystem.dir.rootDir) {
			namesystem.getEditLog().logRemoveUID(src, id.longValue());
			namesystem.getEditLog().logSync();
		}
		return id.longValue();
	}

	 
	public boolean renamePrefix(String prefix, String target) throws IOException {
		boolean result = namesystem.gdt.rename_with_prefix(prefix, target);
		synchronized (namesystem.dir.rootDir) {
			namesystem.getEditLog().logRenameUID(prefix, target);
			namesystem.getEditLog().logSync();
		}
		return result;
 	}

	public boolean rename_with_prefix_without_log(String prefix, String target) throws IOException {
		return namesystem.gdt.rename_with_prefix(prefix, target);
	}

	/**
	 * barrier() return if and only if { 1. there is no pending DT; 2. get
	 * synced on rootDir; }
	 */
	/*public boolean barrier(long timestamp, long hint) throws IOException {
		// Step 1: checking if there are pending DT;
		// Step 2: get synced on rootDir
		return namesystem.barrier(timestamp, hint);
	}*/

	/**
	 * The specification of this method matches that of {@link
	 * getBlockLocations(Path)} except that it does not update the file's access
	 * time.
	 */
	LocatedBlocks getBlockLocationsNoATime(String src, long offset, long length) throws IOException {
		myMetrics.numGetBlockLocations.inc();
		return namesystem.getBlockLocations(src, offset, length, false);
	}

	private static String getClientMachine() {
		String clientMachine = Server.getRemoteAddress();
		if (clientMachine == null) {
			clientMachine = "";
		}
		return clientMachine;
	}

  /** {@inheritDoc} */
  public FsServerDefaults getServerDefaults() throws IOException {
    return namesystem.getServerDefaults();
  }

  /** {@inheritDoc} */
  public void create(String src, 
                     FsPermission masked,
                     String clientName, 
                     EnumSetWritable<CreateFlag> flag,
                     boolean createParent,
                     short replication,
                     long blockSize) throws IOException {
    String clientMachine = getClientMachine();
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.create: file "
                         +src+" for "+clientName+" at "+clientMachine);
    }
    if (!checkPathLength(src)) {
      throw new IOException("create: Pathname too long.  Limit " 
                            + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    namesystem.startFile(src,
        new PermissionStatus(UserGroupInformation.getCurrentUser().getShortUserName(),
            null, masked),
        clientName, clientMachine, flag.get(), createParent, replication, blockSize);
    myMetrics.numFilesCreated.inc();
    myMetrics.numCreateFileOps.inc();
  }

  /** {@inheritDoc} */
  public LocatedBlock append(String src, String clientName) 
      throws IOException {
    String clientMachine = getClientMachine();
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.append: file "
          +src+" for "+clientName+" at "+clientMachine);
    }
    LocatedBlock info = namesystem.appendFile(src, clientName, clientMachine);
    myMetrics.numFilesAppended.inc();
    return info;
  }

  /** {@inheritDoc} */
  public boolean setReplication(String src, short replication) 
    throws IOException {  
    return namesystem.setReplication(src, replication);
  }
    
  /** {@inheritDoc} */
  public void setPermission(String src, FsPermission permissions)
      throws IOException {
    namesystem.setPermission(src, permissions);
  }

  /** {@inheritDoc} */
  public void setOwner(String src, String username, String groupname)
      throws IOException {
    namesystem.setOwner(src, username, groupname);
  }

  @Override
  public LocatedBlock addBlock(String src,
                               String clientName,
                               Block previous,
                               DatanodeInfo[] excludedNodes)
      throws IOException {
    stateChangeLog.debug("*BLOCK* NameNode.addBlock: file "
                         +src+" for "+clientName);
    HashMap<Node, Node> excludedNodesSet = null;
    if (excludedNodes != null) {
      excludedNodesSet = new HashMap<Node, Node>(excludedNodes.length);
      for (Node node:excludedNodes) {
        excludedNodesSet.put(node, node);
      }
    }
    LocatedBlock locatedBlock = 
      namesystem.getAdditionalBlock(src, clientName, previous, excludedNodesSet);
    if (locatedBlock != null)
      myMetrics.numAddBlockOps.inc();
    return locatedBlock;
  }

  /**
   * The client needs to give up on the block.
   */
  public void abandonBlock(Block b, String src, String holder)
      throws IOException {
    stateChangeLog.debug("*BLOCK* NameNode.abandonBlock: "
                         +b+" of file "+src);
    if (!namesystem.abandonBlock(b, src, holder)) {
      throw new IOException("Cannot abandon block during write to " + src);
    }
  }

  /** {@inheritDoc} */
  public boolean complete(String src, String clientName, Block last)
      throws IOException {
    stateChangeLog.debug("*DIR* NameNode.complete: " + src + " for " + clientName);
    CompleteFileStatus returnCode =
      namesystem.completeFile(src, clientName, last);
    if (returnCode == CompleteFileStatus.STILL_WAITING) {
      return false;
    } else if (returnCode == CompleteFileStatus.COMPLETE_SUCCESS) {
      return true;
    } else {
      throw new IOException("Could not complete write to file " + 
                            src + " by " + clientName);
    }
  }

  /**
   * The client has detected an error on the specified located blocks 
   * and is reporting them to the server.  For now, the namenode will 
   * mark the block as corrupt.  In the future we might 
   * check the blocks are actually corrupt. 
   */
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    stateChangeLog.info("*DIR* NameNode.reportBadBlocks");
    for (int i = 0; i < blocks.length; i++) {
      Block blk = blocks[i].getBlock();
      DatanodeInfo[] nodes = blocks[i].getLocations();
      for (int j = 0; j < nodes.length; j++) {
        DatanodeInfo dn = nodes[j];
        namesystem.markBlockAsCorrupt(blk, dn);
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public LocatedBlock updateBlockForPipeline(Block block, String clientName)
      throws IOException {
    return namesystem.updateBlockForPipeline(block, clientName);
  }


  @Override
  public void updatePipeline(String clientName, Block oldBlock,
      Block newBlock, DatanodeID[] newNodes)
      throws IOException {
    namesystem.updatePipeline(clientName, oldBlock, newBlock, newNodes);
  }
  
  /** {@inheritDoc} */
  public void commitBlockSynchronization(Block block,
      long newgenerationstamp, long newlength,
      boolean closeFile, boolean deleteblock, DatanodeID[] newtargets)
      throws IOException {
    namesystem.commitBlockSynchronization(block,
        newgenerationstamp, newlength, closeFile, deleteblock, newtargets);
  }
  
  public long getPreferredBlockSize(String filename) 
      throws IOException {
    return namesystem.getPreferredBlockSize(filename);
  }
    
  /** {@inheritDoc} */
  @Deprecated
  @Override
  public boolean rename(String src, String dst) throws IOException {
    stateChangeLog.debug("*DIR* NameNode.rename: " + src + " to " + dst);
    if (!checkPathLength(dst)) {
      throw new IOException("rename: Pathname too long.  Limit " 
                            + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    boolean ret = namesystem.renameTo(src, dst);
    if (ret) {
      myMetrics.numFilesRenamed.inc();
    }
    return ret;
  }

	public boolean rename_without_log(String src, String dst) throws IOException {
		stateChangeLog.debug("*DIR* NameNode.rename.withoutLog: " + src + " to " + dst);
		if (!checkPathLength(dst)) {
			throw new IOException("rename: Pathname too long. Limit " + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
		}
		boolean ret = namesystem.rename_to_without_log(src, dst);
		if (ret) {
			myMetrics.numFilesRenamed.inc();
		}
		return ret;
	}

	/**
	 * {@inheritDoc}
	 */
	public void concat(String trg, String[] src) throws IOException {
		namesystem.concat(trg, src);
	}

  /** {@inheritDoc} */
  @Override
  public void rename(String src, String dst, Options.Rename... options)
      throws IOException {
    stateChangeLog.debug("*DIR* NameNode.rename: " + src + " to " + dst);
    if (!checkPathLength(dst)) {
      throw new IOException("rename: Pathname too long.  Limit " 
                            + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    namesystem.renameTo(src, dst, options);
    myMetrics.numFilesRenamed.inc();
  }

  /**
   */
  @Deprecated
  public boolean delete(String src) throws IOException {
    return delete(src, true);
  }

  /** {@inheritDoc} */
  public boolean delete(String src, boolean recursive) throws IOException {
		
	
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* Namenode.delete: src=" + src
          + ", recursive=" + recursive);
    }
 		boolean ret = namesystem.delete(src, recursive);
		if (ret)
			myMetrics.numDeleteFileOps.inc();
		 
		return ret;
	}

	// adding by tacitus
	public boolean delete_without_log(String src, boolean recursive) throws IOException {
		if (stateChangeLog.isDebugEnabled()) {
			stateChangeLog.debug("*DIR* Namenode.delete: src=" + src + ", recursive=" + recursive);
		}
		List<String> ls = new ArrayList<String>();
		boolean ret = namesystem.deleteWithoutLog(src, recursive, ls);
		if (ret)
			myMetrics.numDeleteFileOps.inc();
	 
		return ret;
	}

  /**
   * Check path length does not exceed maximum.  Returns true if
   * length and depth are okay.  Returns false if length is too long 
   * or depth is too great.
   * 
   */
  private boolean checkPathLength(String src) {
    Path srcPath = new Path(src);
    return (src.length() <= MAX_PATH_LENGTH &&
            srcPath.depth() <= MAX_PATH_DEPTH);
  }
    
  /** {@inheritDoc} */
  public boolean mkdirs(String src, FsPermission masked, boolean createParent)
      throws IOException {
    stateChangeLog.debug("*DIR* NameNode.mkdirs: " + src);
    if (!checkPathLength(src)) {
      throw new IOException("mkdirs: Pathname too long.  Limit " 
                            + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    return namesystem.mkdirs(src,
        new PermissionStatus(UserGroupInformation.getCurrentUser().getShortUserName(),
            null, masked), createParent);
  }

	// add by tacitus
	public boolean mkdirs_without_log(String src, FsPermission masked, boolean createParent, long now) throws IOException {
		stateChangeLog.debug("*DIR* NameNode.mkdirs: " + src);
		if (!checkPathLength(src)) {
			throw new IOException("mkdirs: Pathname too long.  Limit " + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
		}
		return namesystem.mkdirsWithoutLog(src, new PermissionStatus(UserGroupInformation.getCurrentUser().getShortUserName(), null, masked), createParent, now);
	}

	/**
   */
  public void renewLease(String clientName) throws IOException {
    namesystem.renewLease(clientName);        
  }

  /**
   */
  @Override
  public DirectoryListing getListing(String src, byte[] startAfter)
  throws IOException {
    DirectoryListing files = namesystem.getListing(src, startAfter);
    if (files != null) {
      myMetrics.numGetListingOps.inc();
      myMetrics.numFilesInGetListingOps.inc(files.getPartialListing().length);
    }
    return files;
  }

  /**
   * Get the file info for a specific file.
   * @param src The string representation of the path to the file
   * @return object containing information regarding the file
   *         or null if file not found
   */
  	
  	static long start_ts = -1;
  	static long getFileInfo_ops = -1;
  	static final long nano_sec = 1000000000;
public HdfsFileStatus getFileInfo(String src) throws IOException {
		myMetrics.numFileInfoOps.inc();
		
		NameNode.LOG.info("[dbg] Namenode.getFileInfo: " + src);
		return namesystem.getFileInfo(src, true);
		
 	}

  /**
   * Get the file info for a specific file. If the path refers to a 
   * symlink then the FileStatus of the symlink is returned.
   * @param src The string representation of the path to the file
   * @return object containing information regarding the file
   *         or null if file not found
   */
  public HdfsFileStatus getFileLinkInfo(String src) throws IOException { 
    myMetrics.numFileInfoOps.inc();
    return namesystem.getFileInfo(src, false);
  }
  
  /** @inheritDoc */
  public long[] getStats() {
    return namesystem.getStats();
  }

  /**
   */
  public DatanodeInfo[] getDatanodeReport(DatanodeReportType type)
      throws IOException {
    DatanodeInfo results[] = namesystem.datanodeReport(type);
    if (results == null ) {
      throw new IOException("Cannot find datanode report");
    }
    return results;
  }
    
  /**
   * @inheritDoc
   */
  public boolean setSafeMode(SafeModeAction action) throws IOException {
    return namesystem.setSafeMode(action);
  }

  /**
   * Is the cluster currently in safe mode?
   */
  public boolean isInSafeMode() {
    return namesystem.isInSafeMode();
  }

  /**
   * @throws AccessControlException 
   * @inheritDoc
   */
  public boolean restoreFailedStorage(String arg) 
      throws AccessControlException {
    return namesystem.restoreFailedStorage(arg);
  }

  /**
   * @inheritDoc
   */
  public void saveNamespace() throws IOException {
    namesystem.saveNamespace();
  }

  /**
   * Refresh the list of datanodes that the namenode should allow to  
   * connect.  Re-reads conf by creating new HdfsConfiguration object and 
   * uses the files list in the configuration to update the list. 
   */
  public void refreshNodes() throws IOException {
    namesystem.refreshNodes(new HdfsConfiguration());
  }

  /**
   * Returns the size of the current edit log.
   */
  @Deprecated
  public long getEditLogSize() throws IOException {
    return namesystem.getEditLogSize();
  }

  /**
   * Roll the edit log.
   */
  @Deprecated
  public CheckpointSignature rollEditLog() throws IOException {
    return namesystem.rollEditLog();
  }

  /**
   * Roll the image 
   */
  @Deprecated
  public void rollFsImage() throws IOException {
    namesystem.rollFSImage();
  }
    
  public void finalizeUpgrade() throws IOException {
    namesystem.finalizeUpgrade();
  }

  public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action)
      throws IOException {
    return namesystem.distributedUpgradeProgress(action);
  }

  /**
   * Dumps namenode state into specified file
   */
  public void metaSave(String filename) throws IOException {
    namesystem.metaSave(filename);
  }

  /** {@inheritDoc} */
  public FileStatus[] getCorruptFiles() 
    throws AccessControlException, IOException {
    
    return namesystem.getCorruptFiles();
    
  }
  
  /** {@inheritDoc} */
  public ContentSummary getContentSummary(String path) throws IOException {
    return namesystem.getContentSummary(path);
  }

  /** {@inheritDoc} */
  public void setQuota(String path, long namespaceQuota, long diskspaceQuota) 
      throws IOException {
    namesystem.setQuota(path, namespaceQuota, diskspaceQuota);
  }
  
  /** {@inheritDoc} */
  public void fsync(String src, String clientName) throws IOException {
    namesystem.fsync(src, clientName);
  }

  /** @inheritDoc */
  public void setTimes(String src, long mtime, long atime) 
      throws IOException {
    namesystem.setTimes(src, mtime, atime);
  }

  /** @inheritDoc */
  public void createSymlink(String target, String link, FsPermission dirPerms, 
                            boolean createParent) 
      throws IOException {
    myMetrics.numcreateSymlinkOps.inc();
    /* We enforce the MAX_PATH_LENGTH limit even though a symlink target 
     * URI may refer to a non-HDFS file system. 
     */
    if (!checkPathLength(link)) {
      throw new IOException("Symlink path exceeds " + MAX_PATH_LENGTH +
                            " character limit");
                            
    }
    if ("".equals(target)) {
      throw new IOException("Invalid symlink target");
    }
    final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    namesystem.createSymlink(target, link,
      new PermissionStatus(ugi.getUserName(), null, dirPerms), createParent);
  }

  /** @inheritDoc */
  public String getLinkTarget(String path) throws IOException {
    myMetrics.numgetLinkTargetOps.inc();
    /* Resolves the first symlink in the given path, returning a
     * new path consisting of the target of the symlink and any 
     * remaining path components from the original path.
     */
    try {
      HdfsFileStatus stat = namesystem.getFileInfo(path, false);
      if (stat != null) {
        // NB: getSymlink throws IOException if !stat.isSymlink() 
        return stat.getSymlink();
      }
    } catch (UnresolvedPathException e) {
      return e.getResolvedPath().toString();
    } catch (UnresolvedLinkException e) {
      // The NameNode should only throw an UnresolvedPathException
      throw new AssertionError("UnresolvedLinkException thrown");
    }
    return null;
  }


  ////////////////////////////////////////////////////////////////
  // DatanodeProtocol
  ////////////////////////////////////////////////////////////////
  /** 
   */
  public DatanodeRegistration registerDatanode(DatanodeRegistration nodeReg)
      throws IOException {
    verifyVersion(nodeReg.getVersion());
    namesystem.registerDatanode(nodeReg);
      
    return nodeReg;
  }

  /**
   * Data node notify the name node that it is alive 
   * Return an array of block-oriented commands for the datanode to execute.
   * This will be either a transfer or a delete operation.
   */
  public DatanodeCommand[] sendHeartbeat(DatanodeRegistration nodeReg,
                                       long capacity,
                                       long dfsUsed,
                                       long remaining,
                                       int xmitsInProgress,
                                       int xceiverCount) throws IOException {
    verifyRequest(nodeReg);
    return namesystem.handleHeartbeat(nodeReg, capacity, dfsUsed, remaining,
        xceiverCount, xmitsInProgress);
  }

  public DatanodeCommand blockReport(DatanodeRegistration nodeReg,
                                     long[] blocks) throws IOException {
    verifyRequest(nodeReg);
    BlockListAsLongs blist = new BlockListAsLongs(blocks);
    stateChangeLog.debug("*BLOCK* NameNode.blockReport: "
           +"from "+nodeReg.getName()+" "+blist.getNumberOfBlocks() +" blocks");

    namesystem.processReport(nodeReg, blist);
    if (getFSImage().isUpgradeFinalized())
      return DatanodeCommand.FINALIZE;
    return null;
  }

  public void blockReceived(DatanodeRegistration nodeReg, 
                            Block blocks[],
                            String delHints[]) throws IOException {
    verifyRequest(nodeReg);
    stateChangeLog.debug("*BLOCK* NameNode.blockReceived: "
                         +"from "+nodeReg.getName()+" "+blocks.length+" blocks.");
    for (int i = 0; i < blocks.length; i++) {
      namesystem.blockReceived(nodeReg, blocks[i], delHints[i]);
    }
  }


	public DatanodeCommand blockReportDiff(DatanodeRegistration nodeReg, long[] blocks) throws IOException {
		verifyRequest(nodeReg);
		BlockListAsLongs blist = new BlockListAsLongs(blocks);
		stateChangeLog.info("*BLOCK* NameNode.blockReportDiff: " + "from " + nodeReg.getName() + " " + blist.getNumberOfBlocks() + " blocks");
		namesystem.processReportDiff(nodeReg, blist);
		if (getFSImage().isUpgradeFinalized())
			return DatanodeCommand.FINALIZE;
		return null;
	}

	

	/**
   */
  public void errorReport(DatanodeRegistration nodeReg,
                          int errorCode, 
                          String msg) throws IOException {
    // Log error message from datanode
    String dnName = (nodeReg == null ? "unknown DataNode" : nodeReg.getName());
    LOG.info("Error report from " + dnName + ": " + msg);
    if (errorCode == DatanodeProtocol.NOTIFY) {
      return;
    }
    verifyRequest(nodeReg);
    if (errorCode == DatanodeProtocol.DISK_ERROR) {
      LOG.warn("Volume failed on " + dnName); 
    } else if (errorCode == DatanodeProtocol.FATAL_DISK_ERROR) {
      namesystem.removeDatanode(nodeReg);            
    }
  }
    
  public NamespaceInfo versionRequest() throws IOException {
    return namesystem.getNamespaceInfo();
  }

  public UpgradeCommand processUpgradeCommand(UpgradeCommand comm) throws IOException {
    return namesystem.processDistributedUpgradeCommand(comm);
  }

  /** 
   * Verify request.
   * 
   * Verifies correctness of the datanode version, registration ID, and 
   * if the datanode does not need to be shutdown.
   * 
   * @param nodeReg data node registration
   * @throws IOException
   */
  public void verifyRequest(NodeRegistration nodeReg) throws IOException {
    verifyVersion(nodeReg.getVersion());
    if (!namesystem.getRegistrationID().equals(nodeReg.getRegistrationID()))
      throw new UnregisteredNodeException(nodeReg);
  }
    
  /**
   * Verify version.
   * 
   * @param version
   * @throws IOException
   */
  public void verifyVersion(int version) throws IOException {
    if (version != LAYOUT_VERSION)
      throw new IncorrectVersionException(version, "data node");
  }

  /**
   * Returns the name of the fsImage file
   */
  public File getFsImageName() throws IOException {
    return getFSImage().getFsImageName();
  }
    
  public FSImage getFSImage() {
    return namesystem.dir.fsImage;
  }

  /**
   * Returns the name of the fsImage file uploaded by periodic
   * checkpointing
   */
  public File[] getFsImageNameCheckpoint() throws IOException {
    return getFSImage().getFsImageNameCheckpoint();
  }

  /**
   * Returns the address on which the NameNodes is listening to.
   * @return the address on which the NameNodes is listening to.
   */
  public InetSocketAddress getNameNodeAddress() {
    return rpcAddress;
  }

  /**
   * Returns the address of the NameNodes http server, 
   * which is used to access the name-node web UI.
   * 
   * @return the http address.
   */
  public InetSocketAddress getHttpAddress() {
    return httpAddress;
  }

  NetworkTopology getNetworkTopology() {
    return this.namesystem.clusterMap;
  }

  /**
   * Verify that configured directories exist, then
   * Interactively confirm that formatting is desired 
   * for each existing directory and format them.
   * 
   * @param conf
   * @param isConfirmationNeeded
   * @return true if formatting was aborted, false otherwise
   * @throws IOException
   */
  private static boolean format(Configuration conf,
                                boolean isConfirmationNeeded)
      throws IOException {
    Collection<URI> dirsToFormat = FSNamesystem.getNamespaceDirs(conf);
    Collection<URI> editDirsToFormat = 
                 FSNamesystem.getNamespaceEditsDirs(conf);
    for(Iterator<URI> it = dirsToFormat.iterator(); it.hasNext();) {
      File curDir = new File(it.next().getPath());
      if (!curDir.exists())
        continue;

	isConfirmationNeeded = false; // add by wangyouwei
      if (isConfirmationNeeded) {
        System.err.print("Re-format filesystem in " + curDir +" ? (Y or N) ");
        if (!(System.in.read() == 'Y')) {
          System.err.println("Format aborted in "+ curDir);
          return true;
        }
        while(System.in.read() != '\n'); // discard the enter-key
      }
    }

	FSNamesystem nsys = new FSNamesystem(new FSImage(dirsToFormat, editDirsToFormat), conf);
		long seed = conf.getLong(DFSConfigKeys.DFS_DNN_NAMESPACE_SEED, 710534L);
		nsys.dir.fsImage.formatSeed = seed;
		nsys.dir.fsImage.format(seed);
		LOG.error("Set format seed " + seed + ", get id " + nsys.dir.fsImage.namespaceID);
    return false;
  }

  private static boolean finalize(Configuration conf,
                               boolean isConfirmationNeeded
                               ) throws IOException {
    Collection<URI> dirsToFormat = FSNamesystem.getNamespaceDirs(conf);
    Collection<URI> editDirsToFormat = 
                               FSNamesystem.getNamespaceEditsDirs(conf);
    FSNamesystem nsys = new FSNamesystem(new FSImage(dirsToFormat,
                                         editDirsToFormat), conf);
    System.err.print(
        "\"finalize\" will remove the previous state of the files system.\n"
        + "Recent upgrade will become permanent.\n"
        + "Rollback option will not be available anymore.\n");
    if (isConfirmationNeeded) {
      System.err.print("Finalize filesystem state ? (Y or N) ");
      if (!(System.in.read() == 'Y')) {
        System.err.println("Finalize aborted.");
        return true;
      }
      while(System.in.read() != '\n'); // discard the enter-key
    }
    nsys.dir.fsImage.finalizeUpgrade();
    return false;
  }

  @Override
  public void refreshServiceAcl() throws IOException {
    if (!serviceAuthEnabled) {
      throw new AuthorizationException("Service Level Authorization not enabled!");
    }

    ServiceAuthorizationManager.refresh(
        new Configuration(), new HDFSPolicyProvider());
  }

  @Override
  public void refreshUserToGroupsMappings(Configuration conf) throws IOException {
    LOG.info("Refreshing all user-to-groups mappings. Requested by user: " + 
             UserGroupInformation.getCurrentUser().getShortUserName());
    Groups.getUserToGroupsMappingService(conf).refresh();
  }

  private static void printUsage() {
    System.err.println(
      "Usage: java NameNode [" +
      StartupOption.BACKUP.getName() + "] | [" +
      StartupOption.CHECKPOINT.getName() + "] | [" +
      StartupOption.FORMAT.getName() + "] | [" +
      StartupOption.UPGRADE.getName() + "] | [" +
      StartupOption.ROLLBACK.getName() + "] | [" +
      StartupOption.FINALIZE.getName() + "] | [" +
      StartupOption.IMPORT.getName() + "]");
  }

  private static StartupOption parseArguments(String args[]) {
    int argsLen = (args == null) ? 0 : args.length;
    StartupOption startOpt = StartupOption.REGULAR;
    for(int i=0; i < argsLen; i++) {
      String cmd = args[i];
      if (StartupOption.FORMAT.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.FORMAT;
      } else if (StartupOption.REGULAR.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.REGULAR;
      } else if (StartupOption.BACKUP.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.BACKUP;
      } else if (StartupOption.CHECKPOINT.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.CHECKPOINT;
      } else if (StartupOption.UPGRADE.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.UPGRADE;
      } else if (StartupOption.ROLLBACK.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.ROLLBACK;
      } else if (StartupOption.FINALIZE.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.FINALIZE;
      } else if (StartupOption.IMPORT.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.IMPORT;
      } else
        return null;
    }
    return startOpt;
  }

  private static void setStartupOption(Configuration conf, StartupOption opt) {
    conf.set("dfs.namenode.startup", opt.toString());
  }

  static StartupOption getStartupOption(Configuration conf) {
    return StartupOption.valueOf(conf.get("dfs.namenode.startup",
                                          StartupOption.REGULAR.toString()));
  }

  public static NameNode createNameNode(String argv[], Configuration conf)
      throws IOException {
    if (conf == null)
      conf = new HdfsConfiguration();
    StartupOption startOpt = parseArguments(argv);
    if (startOpt == null) {
      printUsage();
      return null;
    }
    setStartupOption(conf, startOpt);

    switch (startOpt) {
      case FORMAT:
        boolean aborted = format(conf, true);
        System.exit(aborted ? 1 : 0);
        return null; // avoid javac warning
      case FINALIZE:
        aborted = finalize(conf, true);
        System.exit(aborted ? 1 : 0);
        return null; // avoid javac warning
      case BACKUP:
      case CHECKPOINT:
        return new BackupNode(conf, startOpt.toNodeRole());
      default:
        return new NameNode(conf);
    }
  }
    
  /**
   */
  @SuppressWarnings("deprecation")
  public static void main(String argv[]) throws Exception {
    try {
      StringUtils.startupShutdownMessage(NameNode.class, argv, LOG);
      NameNode namenode = createNameNode(argv, null);
      if (namenode != null)
        namenode.join();
    } catch (Throwable e) {
      LOG.error(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

	// **********************************************************************************************************
	/*
	public void echo(String _message_, String _caller_) {
		/*LOG.info("[inf] echo message: " + _message_);
		if (Integer.valueOf(_caller_) < -1) {
			LOG.info("[inf] echo message from CLIENT");
		} else {
			LOG.info("[inf] echo message from " + dnn.getCurrentAddr(Integer.valueOf(_caller_).intValue()));
		}*/
	/*}
	
	public void dumpNamespace() throws IOException{
		LOG.info("[inf] Namenode.dump: output GDT and entire namespace");
		
		StringBuilder _builder = new StringBuilder();
		synchronized (this.namesystem.gdt.gdtmap) {
			for (String key: this.namesystem.gdt.gdtmap.keySet()) {
				//LOG.info("\tPath: " + key + "\tUID: " + this.namesystem.gdt.gdtmap.get(key));
				_builder.append("\tPath: " + key + "\tUID: " + this.namesystem.gdt.gdtmap.get(key) + "\n");
			} 			
		}
		LOG.info("[inf] \nGDT [" + this.namesystem.gdt.gdtmap.keySet().size()+ "]:\n" + _builder.toString());
		_builder.delete(0, _builder.length());
		long sum = this.namesystem.dir.dumpoutRecurisively(_builder, this.namesystem.dir.rootDir) + 1;
		LOG.info("[inf] \n Namespace[" + sum + "]:\n" + _builder.toString());
	}*/


	/*
	 * use the coordinator as the metrics of distributed transactions
	 * */
  @SuppressWarnings("deprecation")
	public void boardcast_final_transaction_status(DTCoordinator coordinator){
		try {
		 
			boolean transaction_result = coordinator.getFinalStatusForTransaction();		
			if (transaction_result) {
				this.namesystem.getEditLog().logWalEdit(coordinator.uuid, DFSConfigKeys.COMMIT_STRING);
			}else {
 				this.namesystem.getEditLog().logWalEdit(coordinator.uuid, DFSConfigKeys.ABORT_STRING);
			}
			
			HashSet<Integer> boardcast_remote_targets = new HashSet<Integer>(coordinator.getCohortList()); 
			if (boardcast_remote_targets.contains(NameNode.selfId)) { 
				//do local first:
				if (transaction_result) {
					//NameNode.LOG.info("[dbg] Namenode.localhost_final_transaction_status: commit: " + NameNode.selfId);
					this.process_msg_cohort(coordinator.uuid, FSOperations.FINAL_COMMIT);
				}else {
					//NameNode.LOG.info("[dbg] Namenode.localhost_final_transaction_status: abort: " + NameNode.selfId);
					this.process_msg_cohort(coordinator.uuid, FSOperations.FINAL_ABORT);
				} 
				boardcast_remote_targets.remove(NameNode.selfId); 
			}
			
			//NameNode.LOG.info("[dbg] dumping boardcast_remote_targets: [coorindator] (after remove selfid) ");
			//for (Integer cohort_id: boardcast_remote_targets) {
				//NameNode.LOG.info("[dbg] cohort_id-> " + cohort_id);
			//}
			final int[] target_hosts = new int[boardcast_remote_targets.size()];
			final DataOutputBuffer[] outputBuffers = new DataOutputBuffer[boardcast_remote_targets.size()];
			final long[] opcodes = new long[boardcast_remote_targets.size()];
			for (int i = 0; i < outputBuffers.length; i++) {
				target_hosts[i] = (Integer)boardcast_remote_targets.toArray()[i];
				outputBuffers[i] = new DataOutputBuffer();
				outputBuffers[i].writeBytes(coordinator.uuid + "\n"); 
				if (transaction_result) {
					opcodes[i] =  FSOperations.FINAL_COMMIT.ordinal();
				}else {
					opcodes[i] =  FSOperations.FINAL_ABORT.ordinal();
					
				}
			}
			
			this.parallel_pb_socket_rpc(target_hosts, opcodes, outputBuffers);
		} catch (Exception e) {
			LOG.error(StringUtils.stringifyException(e));
		}

	}

	

	public void process_msg_coordinator(String transactionID, Integer sender, FSOperations opcode) throws IOException { 		
		DTCoordinator coordinator = null;
		synchronized(coordinator_set){
			coordinator = coordinator_set.get(transactionID);
			if (coordinator == null) {
				//throw new IOException("No DTCoordinator for " + transactionID + " found");
				//NameNode.LOG.info("[dbg] no coordinator for transactionID: " + transactionID);
				//NameNode.LOG.info("[dbg] 1. the coordinator could be removed due to prepare-abort flag");
				//NameNode.LOG.info("[dbg] 2. the transaction is done");
				//NameNode.LOG.info("[dbg] you may want to check storage pool to determine the answer...");
				return;
			}
		}
		
		//NameNode.LOG.info("[dbg] namenode[coordinator]: transId: " + transactionID);
		//NameNode.LOG.info("[dbg] namenode[coordinator]: sender: " + sender);
		//NameNode.LOG.info("[dbg] namenode[coordinator]: opcode: " + opcode);
		
		synchronized (coordinator) {
			coordinator.process_cohort_msg(sender, opcode);
			//if there is any incoming abort, shut down dt immediately;
			if (coordinator.is_aborted() || coordinator.is_ready()) {
				boardcast_final_transaction_status(coordinator);
				synchronized (coordinator_set) {
					if (coordinator_set.containsKey(transactionID))
						coordinator_set.remove(transactionID);
				}

			} else {
				return;
			}

		} 
  
	}
	
	
	public void send_inodefile(String transactionID, int target_host_idx, DataOutputBuffer dob) throws IOException {
		//NameNode.LOG.info("[dbg] sending inode: transaction id: " + transactionID);
		//NameNode.LOG.info("[dbg] sending inode: target host idx: " + target_host_idx);
		long op_code = FSOperations.SEND_INODEFILE.ordinal(); 
		this.pb_non_blocking_socket_rpc(target_host_idx, op_code, dob.getData());	
	}
	
	@SuppressWarnings("deprecation")
	public boolean send_transaction_status_to_coordinator(
			FSOperations opcode, 
			String uuid,
			int coordinator_idx) {
		try {
			if (coordinator_idx == NameNode.selfId) {
				//NameNode.LOG.info("[dbg] send_transaction_status_to_coordinator to localhost");
				this.process_msg_coordinator(uuid, NameNode.selfId, opcode);
				return true;
			}
			
			
			DataOutputBuffer outputBuffer = new DataOutputBuffer();
			outputBuffer.writeBytes(uuid + "\n");
			outputBuffer.writeInt(selfId);			
			DataInputBuffer result = this.pb_non_blocking_socket_rpc(
					coordinator_idx, 
					opcode.ordinal(),
					outputBuffer.getData());
			

			if (result == null) {
				outputBuffer.close();
				return false;
			} else {
				outputBuffer.close();
				return true;
			}
			
		
		} catch (Exception e) {
			LOG.error(StringUtils.stringifyException(e));
			return false;
		}
		
	}
	
	
	public void process_msg_cohort(String transactionID, FSOperations operation) throws IOException {
		
		if (operation == FSOperations.FINAL_COMMIT) { //commit
 			synchronized (cohort_set) {
 				DTCohort dtcohort = cohort_set.get(transactionID);
 				if (dtcohort == null) {
 					LOG.info("[dbg] Exception: dtcohort null for " + transactionID + ":commit");
					return;
				}
 				
				try {
					
				//synchronized (dtcohort) {
					dtcohort.critical_area_lock.lock();
 					dtcohort.setDTAction(FSOperations.FINAL_COMMIT);
					dtcohort.cohort_inside_condition.signal();
				} finally{
					dtcohort.critical_area_lock.unlock();
				}
				//}
			}

		} else if (operation == FSOperations.FINAL_ABORT) {
			// abort all work prepared:
			// including release the lock
 			synchronized (cohort_set) {
				DTCohort dtcohort = cohort_set.get(transactionID);
				if (dtcohort == null) {
					LOG.info("[dbg] Exception: dtcohort null for " + transactionID + ":abort");
					return;
					 
				}
				//synchronized (_dtcohort) {
				dtcohort.critical_area_lock.lock();
				try {
					dtcohort.setDTAction(FSOperations.FINAL_ABORT);
					dtcohort.cohort_inside_condition.signal();
				} finally{
					dtcohort.critical_area_lock.unlock();
				
				}
			}	

		}  
 
	}

	ConcurrentHashMap<String, DTCoordinator> coordinator_set;
	ConcurrentHashMap<String, DTCohort> cohort_set;
			
 	public boolean dtop_mkdir(String uuid, FSOperations opcode, String src, FsPermission permission, boolean createParent) throws UnresolvedLinkException, InterruptedException, IOException {
 		DTCohort dtcohort = null;
		synchronized (cohort_set) {
			dtcohort = cohort_set.get(uuid);
		}		
		if (dtcohort == null) {
			throw new NullPointerException("dtop_mkdir: dtcohort null for transaction: " + uuid);
		}
		 
		//LOG.info("[dbg] Namenode.mkdir: role>" + role + "; path> " + src);
 		boolean result = true;
		if (createParent) {
			if (opcode == FSOperations.MKDIR_DUO_PARENT) {
				result = result && dtcohort.dtop_mkdir_parent(src, permission, true);
			} else if (opcode == FSOperations.MKDIR_DUO_CHILD) {
				result = result && dtcohort.dtop_mkdir_child(src, permission, true);
			}  

		} else {
			if (opcode == FSOperations.MKDIR_DUO_PARENT) {
				result = result && dtcohort.dtop_mkdir_parent(src, permission, false);
			} else if (opcode == FSOperations.MKDIR_DUO_CHILD) {
				result = result && dtcohort.dtop_mkdir_child(src, permission, false);
			}  
		}
		
  
 		//cohort_set.remove(uuid);	
		synchronized (this.namesystem.dir) {
			this.namesystem.dir.notifyAll();
		}
		
		return result;
	}

	public boolean dtop_delete(String uuid, String src, boolean recursive) throws IOException, InterruptedException {
		DTCohort dtcohort = null;
		synchronized (cohort_set) {
			dtcohort = cohort_set.get(uuid);	
		}
		
		if(dtcohort == null)
			throw new NullPointerException("dtop_delete: dtcohort null for transaction: " + uuid);
		
		dtcohort.setTransactionID(uuid);
		boolean result;
		if (recursive) {
			result = dtcohort.dtop_delete(src, true);
		} else {
			result = dtcohort.dtop_delete(src, false);
		}

		synchronized (this.namesystem.dir) {
			this.namesystem.dir.notifyAll();
		}

		if (result) {
			return true;
		} else {
			return false;
		}

	}

	public boolean dtop_rename_file(String _uuid, FSOperations role, String src, String dst) throws IOException, InterruptedException {
		//LOG.info("[inf] NameNode.prepareRenameForFileSrc(String _uuid, String src, String dst)");
		DTCohort dtcohort = null;
		synchronized (cohort_set) {
			dtcohort = cohort_set.get(_uuid);	
		}
		
		if(dtcohort == null)
			throw new NullPointerException("dtop_rename_file: dtcohort null for transaction: " + _uuid);
		
		dtcohort.setTransactionID(_uuid);
		boolean result = true;
		if (role == FSOperations.RENAME_FILE_DUO_SRC) {// this is src
			result = dtcohort.dtop_rename_file_src(src, dst);
		}else if (role == FSOperations.RENAME_FILE_DUO_DST) { // this is dst
			result = dtcohort.dtop_rename_file_dst(src, dst);
		}else{
			throw new IOException("invalid role value: " + role);
			
		} 
 
		//notify part
		synchronized (namesystem.dir) {
			namesystem.dir.notifyAll();
		}
		//cohort_set.remove(_uuid);
		//NameNode.LOG.info("[dbg] Namenode.dtop_rename_file: end");
		return result;
	 
	}

	public boolean dtop_rename_dir(String uuid, String src, String dst, FsPermission _FsPermission) throws IOException, InterruptedException {
		//LOG.info("[inf] Namenode.prepareRenameForDir with src: " + src + "| dst: " + dst);
 		DTCohort dtcohort = null;		
		synchronized (cohort_set) {
			dtcohort = cohort_set.get(uuid);
		}
		
		if (dtcohort == null)
			throw new NullPointerException("dtop_rename_dir: dtcohort null for transaction: " + uuid);
		
 		dtcohort.setTransactionID(uuid);
 		boolean result = dtcohort.dtop_rename_dir(src, dst, _FsPermission);
		
		synchronized (this.namesystem.dir) {
			// wake up everyone who is waiting for the rootdir;			
			this.namesystem.dir.notifyAll();
		}
		
		return result;
		
	 
	}

 
	public void assign_coordinator(String trans_id, Integer[] cohorts) throws SocketTimeoutException, IOException {
	    
 		DTCoordinator dtCoordinator = new DTCoordinator();
 		dtCoordinator.setNamenode(this);
		dtCoordinator.setUUID(trans_id);
		dtCoordinator.setCohortList(cohorts);
		synchronized(coordinator_set){
			coordinator_set.put(trans_id, dtCoordinator);
		}

	}
	 

	//========================================================================================================
	// Protocol session based on GooglePB:
	@SuppressWarnings("deprecation")
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
					// wake up the main thread
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
 			non_blocking_services[target_host_array[i]].processData(controller, request, callback);
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
			LOG.error(StringUtils.stringifyException(e));			
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
	
	@SuppressWarnings("deprecation")
	public DataInputBuffer pb_non_blocking_socket_rpc(int target_host_idx, long op_code, byte[] msg) {
		// Create channel
		try {
			final ReentrantLock lock = new ReentrantLock();
			final Condition sync_condition = lock.newCondition();
			final int zero_idx = 0;
			final ByteString[] response_ref = new ByteString[1];
			final boolean[] rpc_done = new boolean[1];
			//NameNode.LOG.info("[dbg] NameNode: @line " +  (new Throwable().getStackTrace()[0]).getLineNumber());

			response_ref[zero_idx] = null;
			rpc_done[zero_idx] = false;
  			ByteString data = ByteString.copyFrom(msg);
  			//NameNode.LOG.info("[dbg] NameNode: @line " +  (new Throwable().getStackTrace()[0]).getLineNumber());
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
			
			//NameNode.LOG.info("[dbg] NameNode: @line " +  (new Throwable().getStackTrace()[0]).getLineNumber());
 			non_blocking_services[target_host_idx].processData(controller, 
					process_data_request_proto.newBuilder().setOpCode(op_code).setData(data).build(),
					callback); 
 			//NameNode.LOG.info("[dbg] NameNode: @line " +  (new Throwable().getStackTrace()[0]).getLineNumber());
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
				LOG.error(StringUtils.stringifyException(e));
				throw new IOException(e);
			} finally {
				lock.unlock();
			} 
  			
			 // NameNode.LOG.info("[dbg] NameNode: @line " +  (new Throwable().getStackTrace()[0]).getLineNumber());
			//if (controller.failed()) {
 			if (controller.failed()) {
 				//	NameNode.LOG.info("[dbg] pb-rpc-controller fatal error: " + controller.errorText());
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
			// TODO: handle exception
			LOG.error(StringUtils.stringifyException(e));
 		}
		//NameNode.LOG.info("[dbg] NameNode: @line " +  (new Throwable().getStackTrace()[0]).getLineNumber());
 		return null; 
 	} 
	 
	//=====================================================================================================
	
}



@SuppressWarnings("deprecation")
class process_data_service_impl implements BlockingInterface { 
	
	NameNode namenode = null;
	public void set_namenode_ref(NameNode host){
		this.namenode = host;
	}
	@Override
	public process_data_response_proto processData(RpcController controller,
			process_data_request_proto request) throws ServiceException {
		
		try {
			long op_code = request.getOpCode();
			//NameNode.LOG.info("[dbg] pb-socket-rpc received: op_code -> " + op_code);
			DFSConfigKeys.FSOperations operation = DFSConfigKeys.FSOperations.values()[(int)op_code];			
			byte[] data_bytes = request.getData().toByteArray();
			DataInputBuffer dib = new DataInputBuffer();
			dib.reset(data_bytes, data_bytes.length);
			
			switch (operation) {
			case GET_UID:{
				//NameNode.LOG.info("[dbg] namenode.pb-socket-rpc: GET_UID");
	          	String path = dib.readLine();
	          	long  uid = namenode.getUID(path);
 				DataOutputBuffer dob = new DataOutputBuffer();   
 				dob.writeLong(uid);
 				ByteString result = ByteString.copyFrom(dob.getData()); 				
 				process_data_response_proto retobj = 
 						process_data_response_proto.newBuilder().setResult(result).build();
 				dib.close();dob.close();
 				return retobj;				
			}
			case CREATE_UID:{
				
				//NameNode.LOG.info("[dbg] namenode.pb-socket-rpc: CREATE_UID");
				if (NameNode.selfId != DFSConfigKeys.GDT_SERVER_IDX) {
					dib.close();
					throw new IOException("I am not GDT server, reported by host: " + NameNode.selfId);
				}
	          	String path = dib.readLine();
	         	long uid = namenode.createUID(path);
	         	DataOutputBuffer dob = new DataOutputBuffer();   
 				dob.writeLong(uid);
 				ByteString result = ByteString.copyFrom(dob.getData()); 
 				//NameNode.LOG.info("[dbg] namenode.pb-socket-rpc: return " + uid);
 				process_data_response_proto retobj = process_data_response_proto.newBuilder().setResult(result).build();
 				dib.close();dob.close();			
 				return retobj;  
			}
			case RENAME_UID:{
				//NameNode.LOG.info("[dbg] namenode.pb-socket-rpc: RENAME_UID: ");
				if (NameNode.selfId != DFSConfigKeys.GDT_SERVER_IDX) {
					dib.close();
					throw new IOException("Current host is NOT GDT server, hostid#" + NameNode.selfId);
				}
			 
			    String src = dib.readLine();
			    String dst = dib.readLine();
			    //NameNode.LOG.info("[dbg] namenode.pb-socket-rpc: rename_uid: src: " + src); 
			    //NameNode.LOG.info("[dbg] namenode.pb-socket-rpc: rename_uid: dst: " + dst);
				boolean result = namenode.renamePrefix(src, dst);
				
				DataOutputBuffer dob = new DataOutputBuffer();   
 				dob.writeBoolean(result);
 				ByteString ret_str = ByteString.copyFrom(dob.getData()); 
 				process_data_response_proto retobj = process_data_response_proto.newBuilder().setResult(ret_str).build();
 				dib.close();dob.close();
  				return retobj;
				
			}
			/*case DELETE_UID:{
				NameNode.LOG.info("[dbg] namenode.pb-socket-rpc: DELETE_UID");
				if (NameNode.selfId != DFSConfigKeys.GDT_SERVER_IDX) {
					throw new IOException("I am not GDT server, reported by host: " + NameNode.selfId);
				}
				
				String src = dib.readLine();
				NameNode.LOG.info("[dbg] namenode.pb-socket-rpc: delete_uid.SRC: " + src); 
				//boolean result = namenode.removeUID(src);
				boolean result = namenode.removeUID(src);
				
				DataOutputBuffer dob = new DataOutputBuffer();   
 				dob.writeBoolean(result);
 				ByteString ret_str = ByteString.copyFrom(dob.getData()); 
  				return process_data_response_proto.newBuilder().setResult(ret_str).build();
				
			}*/
			case DELETE_PREFIX_UID:{
				//NameNode.LOG.info("[dbg] namenode.pb-socket-rpc: DELETE_PREFIX_UID");
				if (NameNode.selfId != DFSConfigKeys.GDT_SERVER_IDX) {
					dib.close();
					throw new IOException("I am not GDT server, reported by host: " + NameNode.selfId);
				}
				
				String src = dib.readLine();	
				boolean result = namenode.delete_prefix_uid(src);				
				DataOutputBuffer dob = new DataOutputBuffer();   
 				dob.writeBoolean(result);
 				ByteString ret_str = ByteString.copyFrom(dob.getData()); 
 				//NameNode.LOG.info("[dbg] namenode.pb-socket-rpc: delete_prefix_uid......[done]"); 				
 				process_data_response_proto retobj =
 						process_data_response_proto.newBuilder().setResult(ret_str).build();
 				dib.close();dob.close();
 				return retobj;
				
			}
			case GET_FILEINFO:{
				NameNode.LOG.info("[dbg] namenode.pb-socket-rpc: GET_FILEINFO");
				String src = dib.readLine(); 
				NameNode.LOG.info("[dbg] src: " + src);
				HdfsFileStatus hfs = namenode.getFileInfo(src);
				DataOutputBuffer dob = null;
				if (hfs == null) {
					NameNode.LOG.info("[dbg] entry not found, return zero-length buffer");
					dob = new DataOutputBuffer(0);
					
					//NameNode.LOG.info("[dbg] dob.length: " + dob.getLength());
 				}else {
 					NameNode.LOG.info("[dbg] entry found, write HdfsFileStatus data into buffer");
					dob = new DataOutputBuffer();					
					hfs.write(dob);
					NameNode.LOG.info("[dbg] dob.length: " + dob.getLength());
 				}
				
 				ByteString ret_str = ByteString.copyFrom(dob.getData()); 
				NameNode.LOG.info("[dbg] length of buffer returned: " + ret_str.size());
				StringBuilder tmp = new StringBuilder();
				tmp.append("\n");
				int cur = 0;
				for (int i = 0; i < dob.getData().length; i++) {
					tmp.append(dob.getData()[cur++]);
					if (cur % 4 == 0) {
						tmp.append("\n");
					}else {
						tmp.append("\t");	
					}
				}
				 
				process_data_response_proto retobj = 
						process_data_response_proto.newBuilder().setResult(ret_str).build();
				dib.close();dob.close();
  				return retobj; 
				
			}
			case MKDIR_DUO_PARENT:{
				
				//NameNode.LOG.info("[dbg] namenode.pb-socket-rpc: MKDIR_DUO_PARENT:");
				final String uuid = dib.readLine();
				final Integer coordinatoridx = dib.readInt();
				final DTCohort cohort = new DTCohort();
				cohort.setLocalNameNode(namenode);
				cohort.set_coordinator_idx(coordinatoridx);
 				cohort.setTransactionID(uuid);
				namenode.cohort_set.put(uuid, cohort);
				
				
				final String src = dib.readLine(); 
				final short perm = dib.readShort();
				final FsPermission permission = new FsPermission(perm);
				final boolean createParent = dib.readBoolean();
				final boolean result = namenode.dtop_mkdir(uuid, 
							FSOperations.MKDIR_DUO_PARENT, src, permission, createParent);
				
				
				DataOutputBuffer dob = new DataOutputBuffer(); 
	 		    dob.writeBoolean(result);
	 		    ByteString ret_str = ByteString.copyFrom(dob.getData()); 
	 		    namenode.cohort_set.remove(uuid);
	 		    process_data_response_proto retobj =  
	 		    		process_data_response_proto.newBuilder().setResult(ret_str).build();
	 		    dib.close();dob.close();
	 		    return retobj;  
 				
			}case MKDIR_DUO_CHILD:{
				
				//NameNode.LOG.info("[dbg] namenode.pb-socket-rpc: MKDIR_DUO_PARENT: ");
 				final String uuid = dib.readLine();
 				final Integer coordinatoridx = dib.readInt();
				final DTCohort cohort = new DTCohort();
				cohort.setLocalNameNode(namenode);
 				cohort.set_coordinator_idx(coordinatoridx);
				cohort.setTransactionID(uuid);
				namenode.cohort_set.put(uuid, cohort);
 				
				final String src = dib.readLine(); 
				final short perm = dib.readShort();
				final FsPermission permission = new FsPermission(perm);
				final boolean createParent = dib.readBoolean();
				final boolean result = namenode.dtop_mkdir(uuid, 
			    			FSOperations.MKDIR_DUO_CHILD, src, permission, createParent);
	 		    
				DataOutputBuffer dob = new DataOutputBuffer(); 
	 		    dob.writeBoolean(result);
	 		    ByteString ret_str = ByteString.copyFrom(dob.getData()); 
	 		    namenode.cohort_set.remove(uuid);
	 		    process_data_response_proto retobj =  
	 		    		process_data_response_proto.newBuilder().setResult(ret_str).build();
	 		    dib.close();dob.close();
	 		    return retobj;
				
 			}
			case DELETE_DIR:{
				
			 	
			    final String uuid = dib.readLine(); 
			    final Integer coordinatoridx = dib.readInt();
				final DTCohort cohort = new DTCohort();
				cohort.setLocalNameNode(namenode);
 				cohort.set_coordinator_idx(coordinatoridx);
				cohort.setTransactionID(uuid);
				namenode.cohort_set.put(uuid, cohort);
			    
			    
 			    final String src = dib.readLine();
 			    boolean recursive = dib.readBoolean();
 			    NameNode.LOG.info("[dbg] namenode.delete: called by directory branch");
 			    NameNode.LOG.info("[dbg] transaction id: " + uuid);
 			    NameNode.LOG.info("[dbg] src: " + src);
				 
 			    boolean result = namenode.dtop_delete(uuid, src, recursive);
 			    DataOutputBuffer dob = new DataOutputBuffer(); 
	 		    dob.writeBoolean(result);
	 		    ByteString ret_str = ByteString.copyFrom(dob.getData()); 
	 		    namenode.cohort_set.remove(uuid);

	 		    process_data_response_proto retobj = 
	 		    		process_data_response_proto.newBuilder().setResult(ret_str).build();
	 		    dib.close();dob.close();
	 		    return retobj;
							 
				
			} 
			case RENAME_FILE_DUO_SRC:{
				//NameNode.LOG.info("[dbg] namenode.pb-socket-rpc: RENAME_FILE_DUO_SRC: ");
				 

				String uuid = dib.readLine();
			    final Integer coordinatoridx = dib.readInt();
				final DTCohort cohort = new DTCohort();
				cohort.setLocalNameNode(namenode);
 				cohort.set_coordinator_idx(coordinatoridx);
				cohort.setTransactionID(uuid);
				namenode.cohort_set.put(uuid, cohort);
				//NameNode.LOG.info("[dbg] rename_file_src: uuid: " + uuid);
				//NameNode.LOG.info("[dbg] rename_file_src: coordinatoridx: " + coordinatoridx);
				
			    String src = dib.readLine();
			    String dst = dib.readLine();
			    final int dst_host_idx = dib.readInt();
			    cohort.rename_file_dst_host_idx = dst_host_idx;
			    //NameNode.LOG.info("[dbg] rename_file_src: src: " + src);
			    //NameNode.LOG.info("[dbg] rename_file_src: dst: " + dst);
			    //NameNode.LOG.info("[dbg] rename_file_src: dst_host_idx: " + dst_host_idx);
 			    boolean result = namenode.dtop_rename_file(uuid, FSOperations.RENAME_FILE_DUO_SRC, src, dst);
			    
 			    DataOutputBuffer dob = new DataOutputBuffer(); 
	 		    dob.writeBoolean(result);
	 		    ByteString ret_str = ByteString.copyFrom(dob.getData()); 
  			    
			    namenode.cohort_set.remove(uuid);
			    // NameNode.LOG.info("[dbg] rename_file_src: done");
			    process_data_response_proto retobj = 
			    		process_data_response_proto.newBuilder().setResult(ret_str).build();
			    
			    dib.close();dob.close();
	 		    return retobj; 
			    
				
			}
			case RENAME_FILE_DUO_DST:{
				//NameNode.LOG.info("[dbg] namenode.pb-socket-rpc: RENAME_FILE_DUO_DST: ");
				
				String uuid = dib.readLine();
			    final Integer coordinatoridx = dib.readInt();
				final DTCohort cohort = new DTCohort();
				cohort.setLocalNameNode(namenode);
 				cohort.set_coordinator_idx(coordinatoridx);
				cohort.setTransactionID(uuid);
				synchronized (namenode.cohort_set) {
					namenode.cohort_set.put(uuid, cohort);
					namenode.cohort_set.notifyAll();
				}
			 
			    String src = dib.readLine();
			    String dst = dib.readLine();
			    //NameNode.LOG.info("[dbg] rename_file_dst: src: " + src);
			    //NameNode.LOG.info("[dbg] rename_file_dst: dst: " + dst);
			    
 			    boolean result = namenode.dtop_rename_file(uuid,  FSOperations.RENAME_FILE_DUO_DST, src, dst);
			    namenode.cohort_set.remove(uuid);
			     
			    DataOutputBuffer dob = new DataOutputBuffer(); 
	 		    dob.writeBoolean(result);
	 		    ByteString ret_str = ByteString.copyFrom(dob.getData()); 
	 		    process_data_response_proto retobj = 
	 				   process_data_response_proto.newBuilder().setResult(ret_str).build();
	 		    dib.close();dob.close();
	 		    return retobj; 
 				
			}
			case SEND_INODEFILE:{
				try {
					//NameNode.LOG.info("[dbg] namenode.pb-socket-rpc: SEND_INODEFILE: ");
					String transactionID = dib.readLine();
					String dst = dib.readLine();
					//NameNode.LOG.info("[dbg] Namenode.pb-socket-rpc: transaction_id: " + transactionID);
					//NameNode.LOG.info("[dbg] Namenode.pb-socket-rpc: dst: " + dst);
					
					DTCohort cohort = null;
					synchronized (namenode.cohort_set) {
						cohort = namenode.cohort_set.get(transactionID);
						if (cohort == null) {
							namenode.cohort_set.wait();
							cohort = namenode.cohort_set.get(transactionID);
							//	NameNode.LOG.info("[dbg] SEND_INODEFILE: transaction id: " + transactionID);
						}
					}
					//NameNode.LOG.info("[dbg] Namenode.pb-socket-rpc: restore_inodefile_from_dataoutputbuffer...[start]");
					cohort.restore_inodefile_from_dataoutputbuffer(dst, dib);
					//NameNode.LOG.info("[dbg] Namenode.pb-socket-rpc: restore inodefile......[done]");
					
					DataOutputBuffer dob = new DataOutputBuffer(); 
		 		    dob.writeBoolean(true);
		 		    ByteString ret_str = ByteString.copyFrom(dob.getData()); 
		 		 	process_data_response_proto retobj =  
		 				   process_data_response_proto.newBuilder().setResult(ret_str).build();
		 		 	dib.close();dob.close();
		 		    return retobj; 
					
				} catch (Exception e) {
					NameNode.LOG.info(StringUtils.stringifyException(e));
					throw new IOException(e);
					
				} 
				
			}case RENAME_DIR:{
				//NameNode.LOG.info("[dbg] namenode.pb-socket-rpc: rename_dir: ");				 
				String uuid = dib.readLine();
			    final Integer coordinatoridx = dib.readInt();
		 
				final DTCohort cohort = new DTCohort();
				cohort.setLocalNameNode(namenode);
 				cohort.set_coordinator_idx(coordinatoridx);
				cohort.setTransactionID(uuid);
				namenode.cohort_set.put(uuid, cohort); 
			 
 			    String src = dib.readLine();
			    String dst = dib.readLine();
			    short perm_in_short = dib.readShort();
			    FsPermission perm = new FsPermission(perm_in_short);
		 
				boolean result = namenode.dtop_rename_dir(uuid, src, dst, perm);
				namenode.cohort_set.remove(uuid);
			     
				DataOutputBuffer dob = new DataOutputBuffer(); 
		 		dob.writeBoolean(result);
		 		ByteString ret_str = ByteString.copyFrom(dob.getData()); 
		 		process_data_response_proto retobj =  
		 				process_data_response_proto.newBuilder().setResult(ret_str).build();
		 		dib.close();dob.close();
	 		    return retobj; 
 				
			}
			case COORDINATOR:{
				  
				//NameNode.LOG.info("[dbg] namenode.pb-socket-rpc: COORDINATOR: ");
			    String transId = dib.readLine();
			    int memberNumer = dib.readInt();
			    //NameNode.LOG.info("[dbg] namenode.pb-socket-rpc: coordinator: transId: " + transId);
			    //NameNode.LOG.info("[dbg] namenode.pb-socket-rpc: coordinator: memberNumer: " + memberNumer);
			    
			    Integer[] cohorts = new Integer[memberNumer];
			    for (int i = 0; i < memberNumer; i++) {
			    	int memberId =  dib.readInt();
			    	cohorts[i] = memberId;
				}
			    namenode.assign_coordinator(transId, cohorts);
			    
			    DataOutputBuffer dob = new DataOutputBuffer(); 
		 		dob.writeBoolean(true);
		 		ByteString ret_str = ByteString.copyFrom(dob.getData()); 
		 		process_data_response_proto retobj = 
		 				process_data_response_proto.newBuilder().setResult(ret_str).build();
		 		dib.close();dob.close();
	 		    return retobj; 
			    
			}case PREPARE_COMMIT:{
				//cohort -> coordinator
				//NameNode.LOG.info("[dbg] namenode.pb-socket-rpc: PREPARE_COMMIT[coordinator]: ");
			    String transId = dib.readLine();
			    int sender = dib.readInt(); //<-cohort id
 			    
			    try {
			    	namenode.process_msg_coordinator(transId, sender, FSOperations.PREPARE_COMMIT);
	 			    
				    DataOutputBuffer dob = new DataOutputBuffer(); 
			 		dob.writeBoolean(true);
			 		ByteString ret_str = ByteString.copyFrom(dob.getData()); 
			 		//NameNode.LOG.info("[dbg] namenode.pb-socket-rpc: PREPARE_COMMIT[coordinator]: return... ");
			 		process_data_response_proto retobj = 
			 				process_data_response_proto.newBuilder().setResult(ret_str).build();
			 		dib.close();dob.close();
		 		    return retobj; 
				} catch (Exception e) {
					dib.close();
					NameNode.LOG.error(StringUtils.stringifyException(e));					
					throw new IOException(e);
				}
			    
				
			}case PREPARE_ABORT:{
				//NameNode.LOG.info("[dbg] namenode.pb-socket-rpc: PREPARE_ABORT: ");
				 
			    String transId = dib.readLine();
			    Integer sender = dib.readInt();
			    //NameNode.LOG.info("[dbg] Namenode.pb-socket-rpc.PREPARE_ABORT: coordinator: transId: " + transId);
			    //NameNode.LOG.info("[dbg] Namenode.pb-socket-rpc.PREPARE_ABORT: coordinator: sender: " + sender);
			    //abort = 0 
 			    namenode.process_msg_coordinator(transId, sender, FSOperations.PREPARE_ABORT);
 			    DataOutputBuffer dob = new DataOutputBuffer(); 
		 		dob.writeBoolean(true);
		 		ByteString ret_str = ByteString.copyFrom(dob.getData()); 
		 		process_data_response_proto retobj = 
		 				process_data_response_proto.newBuilder().setResult(ret_str).build();
		 		dib.close();dob.close();
	 		    return retobj; 
				
			}case FINAL_COMMIT:{
				//coordinator -> cohort
				//NameNode.LOG.info("[dbg] namenode.pb-socket-rpc: FINAL_COMMIT[Cohort]: ");
			    String transId = dib.readLine();
 			    namenode.process_msg_cohort(transId, FSOperations.FINAL_COMMIT);
 			    
 			    DataOutputBuffer dob = new DataOutputBuffer(); 
		 		dob.writeBoolean(true);
		 		ByteString ret_str = ByteString.copyFrom(dob.getData()); 
		 		process_data_response_proto retobj = 
		 				process_data_response_proto.newBuilder().setResult(ret_str).build();
		 		dib.close();dob.close();
	 		    return retobj; 
				
			}case FINAL_ABORT:{
				NameNode.LOG.info("[dbg] namenode.pb-socket-rpc: FINAL_ABORT_CODE[Cohort]: ");
			    String transId = dib.readLine();
	 		    namenode.process_msg_cohort(transId, FSOperations.FINAL_ABORT);
	 		    
	 		    DataOutputBuffer dob = new DataOutputBuffer(); 
		 		dob.writeBoolean(true);
		 		ByteString ret_str = ByteString.copyFrom(dob.getData()); 
		 		process_data_response_proto retobj = 
		 				process_data_response_proto.newBuilder().setResult(ret_str).build();
		 		dib.close();dob.close();
	 		    return retobj; 
				
			}case ECHO:{
				
				NameNode.LOG.info("[dbg] Namenode.pb-socket-rpc: echo: ");
			    dib.readLine(); //discard the incoming content
			    //NameNode.LOG.info("[dbg] echoing content: " + content);
			    process_data_response_proto retobj =
			    		process_data_response_proto.newBuilder().setResult(ByteString.EMPTY).build();
			    dib.close();
	 		    return retobj; 
				
			}case FS_TEST:{
				try {
					//NameNode.LOG.info("[dbg] namenode.pb-socket-rpc: FS_TEST: ");
					final int test_counter = 10000;
					final FsPermission permission = FsPermission.getDefault();
					final String client_name = dib.readLine();
					final EnumSetWritable<CreateFlag> flag = new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE));
					
					
 					for (int i = 0; i < test_counter; i++) {
						final String file_name = "/" + String.valueOf(i); 
						//public void create(String src,
						//FsPermission masked, 
						//String clientName, EnumSetWritable<CreateFlag> flag, boolean createParent, short replication, long blockSize) throws IOException {						 
						namenode.create(file_name, 
								permission, client_name, flag, false, (short)1, (long)0);
 					}
 					 
 					
					DataOutputBuffer dob = new DataOutputBuffer(); 
			 		dob.writeBoolean(true);
			 		ByteString ret_str = ByteString.copyFrom(dob.getData()); 
			 		process_data_response_proto retobj = 
			 				process_data_response_proto.newBuilder().setResult(ret_str).build();
			 		dib.close();dob.close();
		 		    return retobj; 
					
				} catch (Exception e) {
					dib.close();
					NameNode.LOG.error(StringUtils.stringifyException(e));
					throw new ServiceException(e); 
				}
				
			}default:
				dib.close();
				throw new IOException("unknown operaton received: " + operation);
				//break;
			}
			
		} catch (Exception e) {
			NameNode.LOG.error(StringUtils.stringifyException(e));
			throw new ServiceException(e);
			
		 
		}
		
	}
}
