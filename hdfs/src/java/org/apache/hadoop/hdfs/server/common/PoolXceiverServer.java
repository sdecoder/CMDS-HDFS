package org.apache.hadoop.hdfs.server.common;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;
import org.apache.commons.logging.LogFactory;

/**
 * Server used for receiving/sending data of the file.
 * This is created to listen for requests from pool 
 * clients.  This server is also a RPC server.
 */

class PoolXceiverServer implements Runnable, FSConstants {
	
	public static final Log LOG = LogFactory.getLog(PoolXceiverServer.class);

	ServerSocket ss;
	PoolServer poolserver;
	//Record all sockets opened for data transfer
	Map<Socket, Socket> childSockets = Collections.synchronizedMap(
										new HashMap<Socket, Socket>());
	
	/**
	 * Maximal number of concurrent xceivers per node
	 * Enforcing the limit is required in order to avoid
	 * namenode running out of memory
	 */
	static final int MAX_XCEIVER_COUNT = 256;
	int maxXceiverCount = MAX_XCEIVER_COUNT;
			 
	PoolXceiverServer(ServerSocket ss, Configuration conf,
			PoolServer poolserver) throws IOException {
		
		this.ss = ss;
		this.poolserver = poolserver;
		this.maxXceiverCount = conf.getInt("dfs.namenode.max.xcievers", 
				MAX_XCEIVER_COUNT);
	}
		
	public void run() {
		LOG.info("start thread PoolXceiverServer");
		while (poolserver.shouldRun) {
			try {
				Socket s = ss.accept();
				LOG.info("accept a connection from:" + s.getRemoteSocketAddress().toString());
				s.setTcpNoDelay(true);
				new Thread(poolserver.threadGroup, 
						new PoolXceiver(s, poolserver, this)).start();
			} catch (IOException ie) {
				LOG.warn("PoolXceiverServer:" + StringUtils.stringifyException(ie));
			} catch (Throwable te){
				LOG.error("PoolXceiverServer: Exiting due to:" 
						+ StringUtils.stringifyException(te));
				poolserver.shouldRun = false;
			}		
		}	
		try {
			LOG.info("stop PoolXceiverServer ss socket");
			ss.close();
		} catch (IOException ie) {
			LOG.warn("PoolXceiverServer: " 
					+ StringUtils.stringifyException(ie));
		}
	}
		
	void kill() {
		assert poolserver.shouldRun == false :
			"shouldRun should be set to false before killing";
		try {
			LOG.info("close PoolXceiverServer ss socket");
			this.ss.close();			
		} catch (IOException ie) {
			LOG.warn("PoolXceiverServer.kill(): " 
					+ StringUtils.stringifyException(ie));
		}
		
		//close all the sockets that were accepted earlier
		synchronized (childSockets) {
			for (Iterator<Socket> it = childSockets.values().iterator(); 
				it.hasNext();) {
				Socket thissock = it.next();
				try {
					LOG.info("close socket: " + 
						thissock.getRemoteSocketAddress().toString());
					thissock.close();
				} catch (IOException e) {
					LOG.warn("PoolXceiverserver.kill()");
				}
			}
		}
	}
	
}
