package org.apache.hadoop.hdfs.server.common;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.PoolDataTransferProtocol.FileConstructionStage;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.common.PoolFile.NameNodeInfo;

import org.apache.hadoop.hdfs.server.datanode.ReplicaPoolFile;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import static org.apache.hadoop.hdfs.server.common.PoolDataTransferProtocol.Status.SUCCESS;
import static org.apache.hadoop.hdfs.server.common.PoolDataTransferProtocol.Status.ERROR;


/**
 * Server used for receiving/sending data of the file.
 * This is created to listen for requests from pool 
 * clients.  This server is also a RPC server.
 */

class PoolXceiver extends PoolDataTransferProtocol.Receiver
	implements Runnable, FSConstants {
	
	public static final Log LOG = LogFactory.getLog(PoolXceiver.class);
	
	private final Socket s;
	private final boolean isLocal; //is a logical connection
	private final String remoteAddress; //address of remote site
	private final String localAddress;  //local address of this daemon
	//private final NameNodeInfo node;
	private final PoolXceiverServer poolXceiverServer;
	private final PoolServer server;
	
	private long opStartTime; //the start time of receiving an Op
	Configuration conf;
		
	public PoolXceiver(Socket s, PoolServer server, 
			PoolXceiverServer poolXceiverServer) {
		this.s = s;
		this.isLocal = s.getInetAddress().equals(s.getLocalAddress());
		this.server = server;
		this.poolXceiverServer = poolXceiverServer;
		poolXceiverServer.childSockets.put(s, s);
		remoteAddress = s.getRemoteSocketAddress().toString();
		localAddress = s.getLocalSocketAddress().toString();
		LOG.info("new poolxceiver, local address:" + localAddress + ", remote address:" 
				+ remoteAddress);
	}	
	
	/**
	 * Read/write data from/to the PoolXceiverServer.
	 */
	public void run() {
		DataInputStream in = null;
		try {
			LOG.info("run pool xceiver thread");
			in = new DataInputStream(
					new BufferedInputStream(NetUtils.getInputStream(s), SMALL_BUFFER_SIZE));
				
			final PoolDataTransferProtocol.Op op = readOp(in);
			
			//Make sure the xciver count is not exceeded
			int curXceiverCount = server.getXceiverCount();
			if (curXceiverCount > poolXceiverServer.maxXceiverCount) {
				throw new IOException("xceiverCount " + curXceiverCount 
						+ "exceeds the limit of concurrent xcievers " 
						+ poolXceiverServer.maxXceiverCount);
			}
			opStartTime = PoolServer.now();
			processOp(op, in);
		} catch (Throwable t) {
			LOG.info(server + ": PoolXceiver" ,t);
		} finally {
		    if (LOG.isDebugEnabled()) {
		    	LOG.debug(server + ":Number of active connections is: "
		    			+ server.getXceiverCount());
		     }
		    IOUtils.closeStream(in);
		    IOUtils.closeSocket(s);
		    poolXceiverServer.childSockets.remove(s);
		}
	}
	
	/**
	 * Read file data from the disk
	 */
	protected void opReadFile(DataInputStream in,
			String rootpath, String filename, long startOffset, 
			String client) throws IOException {
		OutputStream baseStream = NetUtils.getOutputStream(s, 
				NameNode.client.writeTimeout);
		DataOutputStream out = new DataOutputStream(
				new BufferedOutputStream(baseStream, SMALL_BUFFER_SIZE));
					
		//send the file
		FileSender fileSender = null;
		try {
			try {
				fileSender = new FileSender(rootpath, filename,
						startOffset, server, client);
			} catch (IOException e) {
				ERROR.write(out); 
				throw e;
			}
			//write ack status
			SUCCESS.write(out);
			//send data
			long read = fileSender.sendFile(out, baseStream);		
		} catch (SocketException ignored) {
		} catch (IOException ioe) {
			LOG.info("Got exception while read file");
			throw ioe;
		} finally {
			IOUtils.closeStream(out);
			IOUtils.closeStream(fileSender);
		}
	}
	
	/**
	 * Write a file to disk using pipeline
	 */
	@Override
	protected void opWriteFile(DataInputStream in,
			String rootpath, String filename,  
			int pipelineSize, FileConstructionStage stage, long newGs,
			String client, NameNodeInfo[] targets) throws IOException {
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("writeFile receive buf size " + s.getReceiveBufferSize() + 
					"tcp no delay " + s.getTcpNoDelay());
		}
		final ReplicaPoolFile rpf = new ReplicaPoolFile(rootpath, filename);
		LOG.info("Receiving file:" + filename + ", remote:" + remoteAddress + 
				" local: " + localAddress);
		
		DataOutputStream replyOut = null;  // stream to prev target
		replyOut = new DataOutputStream(NetUtils.getOutputStream(s, server.socketWriteTimeout));
		FileReceiver filereceiver = null;  // responsible for data handling
		DataOutputStream mirrorOut = null; // stream to next target
		DataInputStream mirrorIn = null;   // reply from next target
		Socket mirrorSock = null; 		   // socket to next target
		String mirrorNode = null;		   // the name:port of next target
		String firstBadLink = ""; 		   //first namenode that failed in connection setup
		PoolDataTransferProtocol.Status mirrorInStatus = SUCCESS;
		
		try {
			if (stage != FileConstructionStage.PIPELINE_CLOSE_RECOVERY) {
				// open a file receiver
				filereceiver = new FileReceiver(rpf, in, 
						s.getRemoteSocketAddress().toString(),
						s.getLocalSocketAddress().toString(),
						stage, newGs, client, server);
			} else {
				//FIXME
				//recoverClose
			}
			
			// Open network conn to backup machine if appropriate
			if (targets.length > 0) {
				InetSocketAddress mirrorTarget = null;
				// Connect to backup machine
				mirrorNode = targets[0].getHostPortName();
				// Connect to backup machine
				mirrorTarget = NetUtils.createSocketAddr(mirrorNode);
				mirrorSock = new Socket();
				try {
					LOG.info("connect to next pipeline node:" + mirrorNode);
					int timeoutValue = server.socketTimeout + 
						HdfsConstants.READ_TIMEOUT_EXTENSION * targets.length;
					int writeTimeout = server.socketWriteTimeout + 
						HdfsConstants.WRITE_TIMEOUT_EXTENSION * targets.length;
					NetUtils.connect(mirrorSock, mirrorTarget, timeoutValue);
					mirrorSock.setSoTimeout(timeoutValue);
					mirrorSock.setSendBufferSize(DEFAULT_DATA_SOCKET_SIZE);
					mirrorOut = new DataOutputStream(
							new BufferedOutputStream(
									NetUtils.getOutputStream(mirrorSock, writeTimeout), 
									SMALL_BUFFER_SIZE));
					mirrorIn = new DataInputStream(NetUtils.getInputStream(mirrorSock));
					
					//Writer op command
					PoolDataTransferProtocol.Sender.opWriteFile(mirrorOut, rootpath, filename,  
							pipelineSize, stage, newGs, client, targets);
					if (filereceiver != null) { //send checksum header
						filereceiver.writeChecksumHeader(mirrorOut);						
					}
					mirrorOut.flush();
					
					//read connect ack: |status|firstbadlink|
					mirrorInStatus = PoolDataTransferProtocol.Status.read(mirrorIn);
					firstBadLink = Text.readString(mirrorIn);
					LOG.info("namenode " + targets.length +
							" got response for connect ack " +
							" from downstream namenode with firstbadlink as " + firstBadLink);
				} catch (IOException e) {
					//send error ack: |status|firstbadlink|
					ERROR.write(replyOut);
					Text.writeString(replyOut, mirrorNode);
					replyOut.flush();
					
					IOUtils.closeStream(mirrorOut);
					mirrorOut = null;
					IOUtils.closeStream(mirrorIn);
					mirrorIn = null;
					IOUtils.closeSocket(mirrorSock);
					mirrorSock = null;
					LOG.info("create pipeline error for connecet ack");
					throw e;
				}		
			}
			
			//send connect ack: |status|firstbadlink|
			mirrorInStatus.write(replyOut);
			Text.writeString(replyOut, firstBadLink);
			replyOut.flush();
			LOG.info("NameNode" + targets.length +	" forwarding connect ack to upstream, ack:"
					 +  mirrorInStatus + ", firstbadlink:" + firstBadLink);
					
			//receive the file and mirror to the next target
			if (filereceiver != null) {
				String mirrorAddr = (mirrorSock == null) ? null : mirrorNode;
				filereceiver.receiveFile(mirrorOut, mirrorIn, replyOut,
						mirrorAddr, targets.length);
			}
			//FIXME
			if (stage == FileConstructionStage.PIPELINE_CLOSE_RECOVERY) {	
			}
		} catch (IOException ioe) {
			LOG.info("receive File " + filename + " received exception " + ioe);
			throw ioe;
		} finally {
			//close all opend streams
			IOUtils.closeStream(mirrorOut);
			IOUtils.closeStream(mirrorIn);
			IOUtils.closeStream(replyOut);
			IOUtils.closeSocket(mirrorSock);
			IOUtils.closeStream(filereceiver);			
		}
		
	}
	
	
	/**
	 * Append to storage pool file, not use pipeline
	 * @param datainputstream is the socket which
	 * receives the command first
	 */	
	protected void opAppendFile(DataInputStream in, 
			String fileName, byte[] b, String cient, 
			NameNodeInfo srcNode, NameNodeInfo[] targets)
		throws IOException {
		
	}
	
	
}
