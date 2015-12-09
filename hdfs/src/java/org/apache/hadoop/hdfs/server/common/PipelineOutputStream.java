package org.apache.hadoop.hdfs.server.common;

import static org.apache.hadoop.hdfs.server.common.PoolDataTransferProtocol.Status.SUCCESS;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import javax.net.SocketFactory;
import java.io.File;
import java.util.ArrayList;
import java.nio.ByteBuffer;
import java.nio.BufferOverflowException;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.PoolDataTransferProtocol.PipelineAck;
import org.apache.hadoop.hdfs.server.common.StorageClover.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.PoolFile.*;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.common.PoolDataTransferProtocol.FileConstructionStage;

/****************************************************************
 * PipelineOutputStream creates files from a stream of bytes.
 *
 * The PoolFileOutputStream which extends FileOutputStream writes 
 * data that is cached internally by this stream. Data is broken up 
 * into packets, each packet is typically 64K in size. A packet 
 * comprises of chunks. Each chunk is typically 512 bytes and has 
 * an associated checksum with it.
 *
 * When up application fills up the currentPacket, it is
 * enqueued into dataQueue.  The DataStreamer thread picks up
 * packets from the dataQueue, sends it to the first namenode in
 * the pipeline and moves it from the dataQueue to the ackQueue.
 * The ResponseProcessor receives acks from the namenodes. When an
 * successful ack for a packet is received from all namenodes, the
 * ResponseProcessor removes the corresponding packet from the
 * ackQueue.
 *
 * In case of error, all outstanding packets and moved from
 * ackQueue. A new pipeline is setup by eliminating the bad
 * namenode from the original pipeline. The DataStreamer now
 * starts sending packets from the dataQueue.
****************************************************************/

class PipelineOutputStream implements Syncable {

	//variables for write file with pipeline
	// closed is accessed by different threads under different locks.
	public static final Log LOG = LogFactory.getLog(PipelineOutputStream.class);
	private Configuration conf;
	private volatile boolean closed = false;
	// each packet 64K, total 5MB
	private static final int MAX_PACKETS = 80; 	 
	private DataChecksum checksum;
	private final LinkedList<Packet> dataQueue = new LinkedList<Packet>();
	private final LinkedList<Packet> ackQueue = new LinkedList<Packet>();
	private Packet currentPacket = null;
	private long currentSeqno = 0;
	private int packetSize = 0; //packet size, including header
	private int chunksPerPacket = 0; //chunks in packet
	private long bytesCurFile = 0;  //bytes written in current file
	public String rootPath; // rootpath
	public String fileName; // file name
	public int writePacketSize;     //default packet size, 64K
	private DataStreamer streamer;
	private long lastFlushOffset = -1; // offset when flush was invoked
	private int socketTimeout;
	private Socket s = null;  				   //pipeline socket to connect namenodes
	
	public static final int SIZE_OF_INTEGER = Integer.SIZE / Byte.SIZE;
	public static final int DEFAULT_DATA_SOCKET_SIZE = 128 * 1024;
	public static final int PIPELINE_WRITE_RETRY = 3;	
	
	/**
	 * Packet Format stored in byte[] buf
	 * |4B Packet length|8B offset|8B seqno|1B islastpacket|
	 * |4B length of data|XB checksum data|YB data|
	 * packet length(pktlen) = 4B + XB + YB
	 * packet size(pktsize) = all
	 */	
	private class Packet {
		ByteBuffer buffer;			//only one of buf and buffer is non-null
		byte[] buf;
		long seqno;					//seqno of buffer in file
		long offsetInFile;			//offset in file
		boolean lastPacketInFile;	//is the last packet in file
		int numChunks;				//number of chunks currently in packet
		int maxChunks;				//max chunks in packet
		int dataStart;				//data start position in packet
		int dataPos;				//data position
		int checksumStart;			//checksum start position in packet
		int checksumPos;			//checksum position
		
		// create a new packet
		Packet(int pktSize, int chunksPerPkt, long offsetInFile) {
			this.lastPacketInFile = false;
			this.numChunks = 0;
			this.offsetInFile = offsetInFile;
			this.seqno = currentSeqno;
			currentSeqno++;
			
			buffer = null;
			buf = new byte[pktSize];
			checksumStart = DataNode.PKT_HEADER_LEN + SIZE_OF_INTEGER;
			checksumPos = checksumStart;
			dataStart = checksumStart + chunksPerPkt * checksum.getChecksumSize();
			dataPos = dataStart;
			maxChunks = chunksPerPkt;
			LOG.info("packet format: |pktlen(4B)|offset(8B)|seqno(8B)|islastpacket(1B)");
			LOG.info("packet format(pktlen=): |datalen(4B)|checksum data(XB)|data(YB)");
			LOG.info("new packet, checksumStart:" + checksumStart + ", checksum len: maxchuns*size=" + maxChunks + "*" + 
				checksum.getChecksumSize()+ "=" + chunksPerPkt * checksum.getChecksumSize()
				+ ", datastart:" + dataStart);
		}
		
		void writeData(byte[] inarray, int off, int len) {
			if (dataPos + len > buf.length) {
				throw new BufferOverflowException();
			}
			System.arraycopy(inarray, off, buf, dataPos, len);
			dataPos += len;
			LOG.info("copy data, buf off:" + off + ", len:" + len
				+ ", packet datapos:" + dataPos);			
		}
		
		void writeChecksum(byte[] inarray, int off, int len) {
			if (checksumPos + len > dataStart) {
				throw new BufferOverflowException();
			}
			System.arraycopy(inarray, off, buf, checksumPos, len);
			checksumPos +=len;
			LOG.info("copy checksum, buf off:" + off + ", len:" + len
				+ ", packet checksumpos:" + checksumPos);
		}
		
		/**
		 * Return ByteBuffer that contains one full packet
		 * This is called only when the packet is ready to be sent
		 */
		ByteBuffer getBuffer() {
			//once this is called, no more data can be added to packet
			if (buffer != null) return buffer;
			//prepare the header and close any gap between checksum and data
			int dataLen = dataPos - dataStart;
			int checksumLen = checksumPos - checksumStart;
			LOG.info("dataPos:" + dataPos + ", dataStart:" + dataStart 
				+ ", dataLen:" + dataLen + ", checksumlen:" + checksumLen);
			if (checksumPos != dataStart) {
				//move the checksum to cover the gap, happen for last packet
				LOG.info("move checksum to cover the gap, from checksumstart:" + 
					checksumStart + " to " + (dataStart-checksumLen) + 
					", len:" + checksumLen);
				System.arraycopy(buf, checksumStart, buf, 
						dataStart-checksumLen, checksumLen);
			} 		
			int pktLen = DFSClient.SIZE_OF_INTEGER + dataLen + checksumLen;
			//normal offset is zero, or set position to offset
			buffer = ByteBuffer.wrap(buf, dataStart-checksumPos,
					DataNode.PKT_HEADER_LEN + pktLen);
			buf = null; 
			buffer.mark(); //mark current position
			//write header and data length
			buffer.putInt(pktLen); //pktSize
			buffer.putLong(offsetInFile);
			buffer.putLong(seqno);
			buffer.put((byte) ((lastPacketInFile) ? 1 : 0));
			buffer.putInt(dataLen);
			LOG.info("packet headerlen: 21, pktlen:" + pktLen + ", offsetinfile:" 
				+ offsetInFile + ", seqno:" + seqno + ", islastpacket:"
				+ lastPacketInFile + ", datalen:" + dataLen);
			
			buffer.reset(); //set to previous position
			return buffer;
		}
		
		//get the packet's last byte's offset in the file
		long getLastByteOffsetFile() {
			return offsetInFile + dataPos - dataStart;
		}
		
		public String toString() {
			return "Packet seqno : " + this.seqno +
			" offsetInFile: " + this.offsetInFile +
			" lastPacketInFile: " + this.lastPacketInFile +
			" lastByteOffsetInFile: " + this.getLastByteOffsetFile();
		}		
	}	
	
	// The DataStreamer class is responsible for sending data packet
	// to other namenodes in the pipeline. And create pipeline when
	// send first packet. Every packet has a sequence number associated 
	// with it. When all the packets for a file are sent out and acks 
	// for each if them are received, the DataStreamer closes the current file.
	class DataStreamer extends Daemon {
		private volatile boolean streamerClosed = false; //if fileStream/fileReplyStream closed
		private File file;
		private DataOutputStream fileStream;  //write stream
		private DataInputStream fileReplyStream; //receive ack stream
		private ResponseProcessor response = null;
		private volatile NameNodeInfo[] nodes = null; //targets
		private ArrayList<NameNodeInfo> excludedNodes = new ArrayList<NameNodeInfo>();
		volatile boolean hasError = false;  //if responder has error
		volatile int errorIndex = -1;
		private FileConstructionStage stage; //file construction stage
			
		/**
		 * Default construction for file create
		 */
		private DataStreamer(NameNodeInfo[] nodes) {
			this.nodes = nodes;
			stage = FileConstructionStage.PIPELINE_SETUP_CREATE;
			LOG.info("enter datastreamer, stage: " + stage);
			for (NameNodeInfo n : this.nodes) {
				LOG.info("pipeline node: " + n.getSocketAddress());
			}
		}
		
		/**
		 * Initialize for data streaming
		 */
		private void initDataStreaming() {
			this.setName("DataStreamer for file " + fileName );
			LOG.info("initialize for response processor");
			response = new ResponseProcessor(nodes);
			response.start();			
			stage = FileConstructionStage.DATA_STREAMING;			
		}
		
	    /*
	     * streamer thread is the only thread that opens streams to namenode, 
	     * and closes them. Any error recovery is also done by this thread.
	     */
		public void run() {
			long lastPacket = System.currentTimeMillis();
			LOG.info("create streamer thread, streamerClosed:" + streamerClosed + 
				", clientrunning:" + NameNode.client.clientRunning);
			
			while (!streamerClosed && NameNode.client.clientRunning) {
				// if the responder encountered an error, shutdown responder
				if (hasError && response != null) {
					try {
						response.close();
						response.join();
						response = null;
					} catch (InterruptedException e) { //do nothing for interrupting
					}
				}
				Packet one = null;
				try {
					// process namenode IO errors if any
					boolean doSleep = false;
					if (hasError && errorIndex >=0) {
						///FIXME?????
						///doSleep = processNamenodeError();
					}
					
					synchronized (dataQueue) {
						//wait for a packet to be sent
						long now = System.currentTimeMillis();
						while ((!streamerClosed && !hasError && NameNode.client.clientRunning && dataQueue.size() == 0 
								&&(stage != FileConstructionStage.DATA_STREAMING ||
								   stage == FileConstructionStage.DATA_STREAMING && now-lastPacket <socketTimeout/2)) 
								|| doSleep) {
							long timeout = socketTimeout/2 - (now - lastPacket);
							timeout = (timeout <=0 || stage != FileConstructionStage.DATA_STREAMING) 
										? 1000 : timeout;
							try {
								LOG.info("data queue wait: " + timeout + ", dataqueue size: " + dataQueue.size()
									 + ", stage: " + stage + ", dosleep:" + doSleep);
								dataQueue.wait(timeout);
							} catch (InterruptedException e) {
								LOG.info("catch Interrupted exception");	
							}
							LOG.info("dataqueue notified closed: " + streamerClosed + ", haserror: " + hasError
									 + ", stage: " + stage + ", dataqueue size:" + dataQueue.size());
							doSleep = false;
							now = System.currentTimeMillis();
						}
						if (streamerClosed || hasError || !NameNode.client.clientRunning 
								|| dataQueue.isEmpty()) {
							//LOG.info("error in streamer streamerclosed: " + streamerClosed + 
							//	", hasError: " + hasError + ", clientrunning: " + 
							//	NameNode.client.clientRunning + ", dataqueue empty: " + dataQueue.isEmpty());
							continue;
						}
						//get packet to be sent, no heartbeat packet
						one = dataQueue.getFirst();
						LOG.info("get packet" + one.seqno + " from dataqueue");
					}
					// create pipeline if necessary
					if (stage == FileConstructionStage.PIPELINE_SETUP_CREATE) {
						LOG.info("create pipeline");
						nodes = createPipeline();
						initDataStreaming();
					}
					if (one.lastPacketInFile) {
						//before send last packet,
						//wait for all data packets have been successfully acked
						synchronized (dataQueue) {
							while (!streamerClosed && !hasError &&
									ackQueue.size() != 0 && NameNode.client.clientRunning) {
								try{
									// wait for acks to arrive from namenodes
									LOG.info("lastpacket wait for sent previous packet, ackqueue size:" + ackQueue.size());
									dataQueue.wait(1000);
								} catch (InterruptedException e) {
								}
							}
						}
						if (streamerClosed || hasError || !NameNode.client.clientRunning) {
							continue;
						}
						stage = FileConstructionStage.PIPELINE_CLOSE;
					}
						
					//send the packet
					ByteBuffer buf = one.getBuffer();
					synchronized (dataQueue) {
						//move packet from dataQueue to ackQueue
						dataQueue.removeFirst();
						ackQueue.addLast(one);
						dataQueue.notifyAll();
					}
						
					//write out data to remote namenode
					LOG.info("write packet" + one.seqno + " buffer to remote node,buf position:" + 
						buf.position() + ", buf remaining(len):" + buf.remaining());
					fileStream.write(buf.array(), buf.position(), buf.remaining());
					fileStream.flush();
					
					lastPacket = System.currentTimeMillis();
					if (streamerClosed || hasError || !NameNode.client.clientRunning) {
						continue;
					}
					
					// wait for the close packet has been acked
					if (one.lastPacketInFile) {
						synchronized (dataQueue) {
							while (!streamerClosed && !hasError &&
									ackQueue.size() != 0 && NameNode.client.clientRunning) {
									dataQueue.wait(1000);
							}
						}
						if (streamerClosed || hasError || !NameNode.client.clientRunning) {
								continue;
						}
						endFile();
					}		
					
				} catch (Throwable e) {
					hasError = true;
					if (errorIndex == -1){ //not a namenode error
						streamerClosed = true;
					}
				}
			}
			closeInternal();
		}

		private void endFile() {
			this.setName("DataStreamer for file " + fileName);
			LOG.info("end file, close data streamer");
			streamerClosed = true;
			closeResponder();
			closeStream();
			nodes = null;
			stage = FileConstructionStage.PIPELINE_SETUP_CREATE;
		}
	
		void close(boolean force) {
			streamerClosed = true;
			synchronized (dataQueue) {
				dataQueue.notifyAll();
			}
 			if (force) {
				LOG.info("interrupt streamer");
				this.interrupt();
			} 
		}
	
		private void closeInternal() {
			close(true);
			closeResponder(); //close,interrupt and join
			closeStream();
			streamerClosed = true;
			closed = true;
			synchronized (dataQueue) {
				dataQueue.notifyAll();
			}
		}
				
		private void closeResponder() {
			if (response != null) {
				LOG.info("close responder");
				try {
					response.close();
					response.join();
				} catch (InterruptedException e) {					
				} finally {
					response = null;
				}
			}
		}
		
		private void closeStream() {
			if (fileStream != null) {
				LOG.info("close stream");
				try{
					fileStream.close();
				} catch (IOException e) {					
				} finally {
					fileStream = null;
				}
			}
			if (fileReplyStream != null) {
				try {
					fileReplyStream.close();
				} catch (IOException e) {
				} finally {
					fileReplyStream = null;
				}
			}
		}
		
	    /**
	     * Open a DataOutputStream to a NameNode so that it can be written to.
	     * This happens when a file is created.
	     */
		private NameNodeInfo[] createPipeline() throws IOException {
			int count = PIPELINE_WRITE_RETRY;
			LOG.info("enter create pipeline, write retries: " + count);
			boolean success = false;
			do {
				hasError = false;
				errorIndex = -1;
				success = false;
				NameNodeInfo[] w = excludedNodes.toArray(
						new NameNodeInfo[excludedNodes.size()]);
				
				//connect to first NameNode in the list
				success = createFileOutputStream(nodes, 0L, false);
				if (!success) {
					LOG.info("Excluding namenode: " + nodes[errorIndex].getHostName());
					excludedNodes.add(nodes[errorIndex]);
					//FIXME delete error nodes
					
				}
			} while (!success && --count >=0);	
			if (!success) {
				throw new IOException("Unable to create new file");
			}
			return nodes;
		}
		
		
	    // connects to the first namenode in the pipeline
	    // Returns true if success, otherwise return failure.
	    //
		private boolean createFileOutputStream(NameNodeInfo[] nodes, 
				long newGs, boolean recoveryFlag) {
			PoolDataTransferProtocol.Status pipelineStatus = SUCCESS;
			String firstBadLink = "";
			
			try {
				InetSocketAddress target = NetUtils.createSocketAddr(nodes[0].getHostPortName());
				s =  new Socket();
				int timeoutValue = HdfsConstants.READ_TIMEOUT_EXTENSION * nodes.length +
							       		socketTimeout; 
				NetUtils.connect(s, target, timeoutValue);
				//set read timeout
				s.setSoTimeout(timeoutValue); 
				s.setSendBufferSize(DEFAULT_DATA_SOCKET_SIZE);
				long writeTimeout = NameNode.client.writeTimeout + 
						HdfsConstants.WRITE_TIMEOUT_EXTENSION * nodes.length;
				LOG.info("connect to: " + target.getAddress().toString() + ", writetimeout:" + writeTimeout);
				DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
			        	NetUtils.getOutputStream(s, writeTimeout), DataNode.SMALL_BUFFER_SIZE));
			    	fileReplyStream = new DataInputStream(NetUtils.getInputStream(s));
				
			    	//xmit header info to namenode
			    	// write command
			    	PoolDataTransferProtocol.Sender.opWriteFile(out, rootPath,
			    		fileName, nodes.length, recoveryFlag?stage.getRecoveryStage():stage,
			    		newGs, NameNode.client.clientName, nodes);
			    	// write type(crc32)|bytesPerChecksum
			    	checksum.writeHeader(out); 
				out.flush();
				
				//receive ack for connect: |status|firstbadlink|
				pipelineStatus = PoolDataTransferProtocol.Status.read(fileReplyStream);
				firstBadLink = Text.readString(fileReplyStream);
				LOG.info("receive create ack from:" + s.getRemoteSocketAddress().toString()+ 
					" pipelinestatus:" + pipelineStatus + ", firstBadLink:" + firstBadLink);
				if (pipelineStatus != SUCCESS) {
					throw new IOException("Bad connect ack with firstBadLink as "
							+ firstBadLink);
				}
				fileStream = out;
				return true; //success
			} catch (IOException ie) {
				LOG.info("Exception in createFileOutputStream" + ie);
				//find the namenode that matches, firstBadLink !=""
				if (firstBadLink.length() != 0) {
					for (int i = 0; i< nodes.length; i++) {
						if (nodes[i].getHostName().equals(firstBadLink)) {
							errorIndex = i;
							break;
						}
					}
				} else {
					errorIndex = 0;
				}
				hasError = true;
				fileReplyStream = null;
				return false;  //error
			}
		}
		
		
	    // Processes reponses from namenodes.  A packet is removed 
	    // from the ackQueue when its response arrives.
	    //
		private class ResponseProcessor extends Daemon {
			private volatile boolean responderClosed = false;
			private NameNodeInfo[] targets = null;
			private boolean isLastPacketInFile = false;
			
			ResponseProcessor (NameNodeInfo[] targets) {
				this.targets = targets;
			}
			
			public void run() {
				setName("ResponseProcessor for file " + fileName);
				PipelineAck ack = new PipelineAck();
				LOG.info("start response processor thread, islastpacket:" + isLastPacketInFile);
				while (!responderClosed && NameNode.client.clientRunning && !isLastPacketInFile) {
					//process response from namenodes
					try {
						//read an ack from the pipeline: seqno|replies
						ack.readFields(fileReplyStream);
						long seqno = ack.getSeqno();
						//process response status from namenodes
						for (int i= ack.getNumOfReplies() -1; i >=0 && NameNode.client.clientRunning; i--) {
							final PoolDataTransferProtocol.Status reply = ack.getReply(i);
							if (reply != SUCCESS) {
								errorIndex = i; //first bad namenode
								throw new IOException("Bad response " + reply +
										"for file " + fileName + "from namenode " + targets[i].getHostName());
							}
						}
						assert seqno != PipelineAck.UNKOWN_SEQNO;
						
						// a succcess ack for a data packet
						Packet one = null;
						synchronized(dataQueue) {
							one = ackQueue.getFirst();
						}
						if (one.seqno != seqno) {
							throw new IOException("Responseprocessor: Expecting seqno " +
									"for file" + fileName + one.seqno + " but received " +seqno);
						}
						isLastPacketInFile = one.lastPacketInFile;
						synchronized (dataQueue) {
							LOG.info("remove pacaket" + one.seqno + " from ackqueue"
								+ ", islastpacket:" + isLastPacketInFile);
							ackQueue.removeFirst();
							dataQueue.notifyAll();
						}						
					} catch (Exception e) {
						if (!responderClosed) {
							hasError = true;
							errorIndex = errorIndex==-1 ? 0 : errorIndex;
							synchronized (dataQueue) {
								dataQueue.notifyAll();
							}
							responderClosed = true;
						}
					}
				}
			}	
			
			void close() {
				responderClosed = true;
				this.interrupt();
			}
			
		}
	}
	
	public PipelineOutputStream(Configuration conf, int socketTimeout, 
			String rootpath, String filename, 
			int bytesPerChecksum, int writePacketSize, NameNodeInfo[] nodes)
			throws IOException{
		this.conf = conf;
		this.socketTimeout = socketTimeout;
		this.writePacketSize = writePacketSize;
		LOG.info("socketTimeout: " + this.socketTimeout + ", writepacket size: " 
			+ this.writePacketSize + ", bytesperchecksum: " + bytesPerChecksum);
		this.checksum = DataChecksum.newDataChecksum(DataChecksum.CHECKSUM_CRC32,
				bytesPerChecksum);
		
		this.rootPath = rootpath;
		this.fileName = filename;

		LOG.info("pipeline file, root: " + this.rootPath + ", name: " + this.fileName);
		computePacketChunkSize(writePacketSize, bytesPerChecksum);
		
		streamer = new DataStreamer(nodes);
		streamer.start();
	} 
	
	private void computePacketChunkSize(int psize, int csize) {
	    int chunkSize = csize + checksum.getChecksumSize(); //516B
	    int n = DataNode.PKT_HEADER_LEN + SIZE_OF_INTEGER;  //25B
	    chunksPerPacket = Math.max((psize - n + chunkSize-1)/chunkSize, 1);
	    packetSize = n + chunkSize*chunksPerPacket;
	    LOG.info("chunks per packet: " + chunksPerPacket + ", packetSize: " + packetSize);
    }
	
	
	private void waitAndQueuePacket(Packet packet) throws IOException {
		synchronized (dataQueue) {
			// If queue is full, then wait till have engough space
			while (!closed && dataQueue.size() + ackQueue.size() > 
				MAX_PACKETS) {
				try {
					LOG.info("too more packet in queue, need wait");
					dataQueue.wait();
				} catch (InterruptedException e){
				}
			}
			if (closed) throw new IOException("pipeline output stream is closed");
			queuePacket(packet);
		}
	}
	
	private void queuePacket(Packet packet) {
		synchronized (dataQueue) {
		    LOG.info("add packet " + packet.seqno + " to dataqueue and notify");
		    dataQueue.addLast(packet);
		    dataQueue.notifyAll();
		}
	}
	
	//enqueue packet to dataqueue for pipeline write
	public void writeChunk(byte[] b, int offset, int len, byte[] checksum,
			List<NameNodeInfo> nodes, boolean isLastPacket) throws IOException {
		if (!NameNode.client.clientRunning) {
	        	throw new IOException("Filesystem closed");
	    	}
		int cklen = checksum.length;
		int bytesPerChecksum = this.checksum.getBytesPerChecksum();
		if (len > bytesPerChecksum || cklen != this.checksum.getChecksumSize()) {
			throw new IOException("writeChunk() parameters do not fit with supported");
		}
		//no data written when close, send an empty packet
		if(isLastPacket && len ==0) {
			LOG.info("send empty packet, isLastPacket:" + isLastPacket);
			currentPacket = new Packet(DataNode.PKT_HEADER_LEN + SIZE_OF_INTEGER, 0, bytesCurFile);
			currentPacket.lastPacketInFile = true;
			waitAndQueuePacket(currentPacket);
			currentPacket = null;
			bytesCurFile = 0;
			lastFlushOffset = -1;
			return;			
		}
		
		if (currentPacket == null) {
			currentPacket = new Packet(packetSize, chunksPerPacket,
					bytesCurFile);			
		}
		currentPacket.writeChecksum(checksum, 0, cklen);
		currentPacket.writeData(b, offset, len);
		currentPacket.numChunks++;
		bytesCurFile += len;	
		
		//if packet is full, enqueue it for transmission
		if (currentPacket.numChunks == currentPacket.maxChunks || isLastPacket)  {
			LOG.info("wait and queue packet, numchunks:" +currentPacket.numChunks + ", islastpacket:" + 
					isLastPacket);
			waitAndQueuePacket(currentPacket);
			currentPacket = null;	
						
			//if encountering file tail, send an empty packet
			if (isLastPacket) {
				LOG.info("send empty packet, isLastPacket:" + isLastPacket);
				currentPacket = new Packet(DataNode.PKT_HEADER_LEN + SIZE_OF_INTEGER, 0, bytesCurFile);
				currentPacket.lastPacketInFile = true;
				waitAndQueuePacket(currentPacket);
				currentPacket = null;
				bytesCurFile = 0;
				lastFlushOffset = -1;
			}
		}
	}


	//shutdown datastreamer and responseprocessor threads
	//interrupt datastreamer if force is true
	public void joinStreamer() throws IOException {
	  try {
		LOG.info("streamer join");
		streamer.join();
	  } catch (InterruptedException e) {
		LOG.info("interrupted, close thread");	
	  } finally {
		if (s != null) {
			s.close();
		}
		streamer = null;
		s = null;
		closed = true;
	  }
	}
	
	@Override
	@Deprecated
	public synchronized void sync() throws IOException {
		hflush();
	}
		
	///FIMXME ?????
	@Override
	public synchronized void hflush() throws IOException {
		
	}
	
	@Override
	public synchronized void hsync() throws IOException {
		hflush();
	}
	

}
