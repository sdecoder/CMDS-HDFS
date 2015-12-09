package org.apache.hadoop.hdfs.server.common;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.zip.Checksum;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSInputChecker;
import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.common.PoolDataTransferProtocol.FileConstructionStage;
import org.apache.hadoop.hdfs.server.common.PoolDataTransferProtocol.Status;
import org.apache.hadoop.hdfs.server.common.PoolDataTransferProtocol.PipelineAck;
import org.apache.hadoop.hdfs.server.common.PoolFile.NameNodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hdfs.server.common.PoolFile;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.hdfs.server.datanode.FSDatasetInterface.BlockInputStreams;
import org.apache.hadoop.hdfs.server.datanode.FinalizedPoolReplica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaPoolFile;
import org.apache.hadoop.hdfs.server.datanode.ReplicaPoolInfo;
import org.apache.hadoop.hdfs.server.datanode.ReplicaPoolPipeline;
import org.apache.hadoop.hdfs.server.datanode.ReplicaPoolUnderRecovery;
import org.apache.hadoop.hdfs.server.datanode.FSDatasetInterface.BlockWriteStreams;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipelineInterface;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import static org.apache.hadoop.hdfs.server.common.PoolDataTransferProtocol.Status.SUCCESS;
import static org.apache.hadoop.hdfs.server.common.PoolDataTransferProtocol.Status.ERROR;

/** A class that receives a file and write to its own disk, 
 * meanwhile may copies it to another site. 
 * 
 **/

class FileReceiver implements java.io.Closeable, FSConstants {
	public static final Log LOG = LogFactory.getLog(FileReceiver.class);
	
	private ReplicaPoolFile rpf;
	private long len; // file length
	private long generalstamp;
	private DataInputStream in = null; //from where data are read
	private DataChecksum checksum; 
	private OutputStream out = null; //to file at local disk
	private DataOutputStream checksumOut = null; //to meta file at local disk
	private int bytesPerChecksum;
	private int checksumSize;
	private ByteBuffer buf; //contains one full packet
	private int bufRead; 	//amount of valid data in the buf
	private int maxPacketReadLen;
	protected final String inAddr;
	protected final String myAddr;
	private String mirrorAddr;
	private DataOutputStream mirrorOut;
	private Daemon responder = null;
	// throttler
	private BlockWriteStreams streams;
	private String clientName;
	private Checksum partialCrc = null;
	private final PoolServer server;
	private ReplicaInPipelineInterface replicaInfo;
	volatile private boolean mirrorError;

	FileReceiver(ReplicaPoolFile rpf, DataInputStream in, String inAddr,
			String myAddr, FileConstructionStage stage, long newGs,
			String clientName,PoolServer namenode)
			throws IOException {
		try{
			this.rpf = rpf;
			this.in = in;
			this.inAddr = inAddr;
			this.myAddr = myAddr;
			this.clientName = clientName;
			this.server = namenode;
			LOG.info("create filereceiver, remote:" + inAddr + ", local:" + myAddr 
				+ ", clientname:" + clientName);
			
			
			//open local disk out
			switch(stage) {
				case PIPELINE_SETUP_CREATE:
					LOG.info("state: pipeline_setup_create");
					replicaInfo = createTmp(FileType.RBW_FILE);
					break;
				case PIPELINE_SETUP_STREAMING_RECOVERY:
					//FIXME
					//replicaInfo = recoverRbw();
					break;
				case PIPELINE_SETUP_APPEND:
					//FIXME
					//replicaInfo = append();
					break;
				case PIPELINE_SETUP_APPEND_RECOVERY:
					//FIXME
					//replicaInfo = recoveryAppend();
					break;
				default: throw new IOException("Unsupported stage " + stage + 
						"while receiving file " + rpf + " from " + inAddr);	
			}
			//read checksum meta information
			this.checksum = DataChecksum.newDataChecksum(in);
			this.bytesPerChecksum = checksum.getBytesPerChecksum();
			this.checksumSize = checksum.getChecksumSize();
			LOG.info("receive checksum meta, bytesperchecksum:" + bytesPerChecksum
					 + ", checksumsize:" + checksumSize);			

			boolean isCreate = stage == FileConstructionStage.PIPELINE_SETUP_CREATE;
			streams = replicaInfo.createStreams(isCreate, 
					this.bytesPerChecksum, this.checksumSize);
			if (streams != null){
			   this.out = streams.getDataOut();
			   this.checksumOut = new DataOutputStream(new BufferedOutputStream(
                           streams.getChecksumOut(), 
                           SMALL_BUFFER_SIZE));
				
				// checksum meta file header |2B(version)|1B(type)|4B(bytesperchecksum)
			    if (isCreate) {
			    	BlockMetadataHeader.writeHeader(checksumOut, checksum);
			    }   
			}			
		} catch (IOException ioe) {
			IOUtils.closeStream(this);
			throw ioe;
		}
	}
	
	void writeChecksumHeader(DataOutputStream mirrorOut) throws IOException {
		checksum.writeHeader(mirrorOut);
	}
	
	public enum FileType {
		TEM_FILE("tmp"),
		RBW_FILE("rbw");
		
		public final String dir;
		private FileType(String dir) {this.dir = dir;}
		public String getName() {return dir;}
	}
	   
	public synchronized ReplicaInPipelineInterface createTmp(FileType type)
		throws IOException {
		ReplicaPoolInfo replicaInfo = server.map.get(rpf.getFilename());
		if (replicaInfo != null) {
			   throw new IOException("file " + rpf +
				          " already exists in state " + replicaInfo.getState() +
				          " and thus cannot be created.");
		}
		
		// create a temporary file to hold file
		File f = new File(rpf.getRootpath(), rpf.getFilename());
		File tmpDir = new File(f.getParentFile() + "/" + type.getName());
		if (tmpDir.exists()) {
			FileUtil.fullyDelete(tmpDir);
		}		
		if (!tmpDir.mkdirs()) {
			if (!tmpDir.isDirectory()) {
				throw new IOException("Mkdirs failed to create " + tmpDir.toString());
			}
		}
		File tmpfile = new File(tmpDir, rpf.getName());
		if (tmpfile.exists()) {
			throw new IOException("Unexcepted problem in creating temporary file for " +
					tmpfile + ". File " + rpf.getName() + "should not be present, but is.");
		}
		//Create the zero-length tmp file
		boolean fileCreated = false;
		try {
			fileCreated = tmpfile.createNewFile();
		} catch (IOException ioe) {
			throw ioe;
		}
		if (!fileCreated) {
			throw new IOException("Unexcepted problem in creating temporary file. " + 
					"File " + tmpfile + "should be creatable, but is already present.");
		}
		LOG.info("create zero-length tmp file:" + tmpfile.getPath());
		
		ReplicaPoolPipeline newReplicaInfo = new ReplicaPoolPipeline(rpf.getRootpath(),
				rpf.getFilename(), tmpfile.getParentFile());
		server.map.put(rpf.getFilename(), newReplicaInfo);
		return newReplicaInfo;		
	}
	
	void receiveFile(
			DataOutputStream mirrOut, // output to next namenode
			DataInputStream mirrIn,  // input from next namenode
			DataOutputStream replyOut, // output to previous namenode
			String mirrAddr, int numTargets) throws IOException {
		
		  	boolean responderClosed = false;
		  	this.mirrorOut = mirrOut;
		  	this.mirrorAddr = mirrAddr;
			LOG.info("prepare to receive file, next addr:" + mirrAddr
				+ ", numtargets:" + numTargets);
		  	
		  	try {
		  		//FIXME can be commented when test firstly
		  		responder = new Daemon(server.threadGroup, 
		  				new PacketResponder(this, rpf,
		  						mirrIn, replyOut, numTargets,
		  						Thread.currentThread()));
		  			responder.start(); // start thread to process response 			
		  				  		
		  		/**
		  		 * Receiver non-zero if not the last packet
		  		 */
		  		while (receivePacket() >=0) {}
		  		
		  		// wait for all outstanding packet responses. And then
		        // indicate responder to gracefully shutdown.
		        // Mark that responder has been closed for future processing
		  		if (responder != null) {
		  			((PacketResponder)responder.getRunnable()).close();
		  			responderClosed = true;
		  		}
		  	} catch (IOException ioe) {
		  		LOG.info("Exception in receiveFile from file " + rpf + 
		  				" " + ioe);
		  		throw ioe;
		  	} finally {
		  		if (!responderClosed) { //Abnormal termination of the flow above
		  			IOUtils.closeStream(this);
		  			if (responder != null) {
		  				responder.interrupt();
		  			}
		  		}
		  		if (responder != null) {
		  			try {
		  				responder.join();
		  			} catch (InterruptedException e) {
		  				throw new IOException("Interrupted receiveFile");
		  			}
		  			responder = null;
		  		}
		  	}
	}
	
	/** 
	  * Receives and processes a packet. It can contain many chunks.
	  * returns the number of data bytes that the packet has.
	  */
	private int receivePacket() throws IOException {
		// read the next packet
		readNextPacket();
		
		//FIXME buf.position at this moment?
		buf.mark();
		//read the header
		buf.getInt(); //packet length
		long offsetInFile = buf.getLong(); //get offset of packet in file
		
		if (offsetInFile > replicaInfo.getNumBytes()) {
			throw new IOException("Received an out-of-sequence packet for " + rpf + 
			          "from " + inAddr + " at offset " + offsetInFile +
			          ". Expecting packet starting at " + replicaInfo.getNumBytes());
		}
		long seqno = buf.getLong(); //get seqno
		boolean lastPacketInFile = (buf.get() != 0);
		int len = buf.getInt();		//length of data
		LOG.info("read packet" + seqno + ", offset in file:" + offsetInFile + ", lastpacket in file:" 
			+ lastPacketInFile + ", len: " + len);
		
		if (len < 0) {
			 throw new IOException("Got wrong length during writeFile(" + rpf + 
                     ") from " + inAddr + " at offset " + 
                     offsetInFile + ": " + len); 
		}
		int endOfHeader = buf.position();
		buf.reset();
		
		return receivePacket(offsetInFile, seqno, lastPacketInFile, len, endOfHeader);
		
	}
	
	/** 
	  * Receives and processes a packet. It can contain many chunks.
	  * returns the number of data bytes that the packet has.
	  */
	private int receivePacket(long offsetInFile, long seqno, 
			boolean lastPacketInFile, int len, int endOfHeader) throws IOException {
		
		//update received bytes
	    long firstByteInFile = offsetInFile;
	    offsetInFile += len;
	    if (replicaInfo.getNumBytes() < offsetInFile) {
	      replicaInfo.setNumBytes(offsetInFile);
	    }	
	    
	    ///FIXME can be commented in first test
	    if (responder != null) {
	    	((PacketResponder)responder.getRunnable()).enqueue(seqno, 
	    			lastPacketInFile, offsetInFile);
	    }
	    
	    //First write the packet to the mirror, flush local later
	    if (mirrorOut != null && !mirrorError) {
	    	try {
	    		mirrorOut.write(buf.array(), buf.position(), buf.remaining());
	    		mirrorOut.flush();
	    	} catch (IOException e) {
	    		handleMirrorOutError(e);
	    	}
	    }
	    
	    buf.position(endOfHeader); //position at: |checksumdata|data|
	    
	    if(lastPacketInFile || len ==0) {
	    	LOG.info("Receiving last empty packet or the end of the file " + rpf);
	    } else {
	    	 int checksumLen = ((len + bytesPerChecksum - 1)/bytesPerChecksum)*
             						checksumSize;
	    	 if ( buf.remaining() != (checksumLen + len)) {
	    	        throw new IOException("Data remaining in packet does not match" +
	    	                              "sum of checksumLen and dataLen " +
	    	                              " size remaining: " + buf.remaining() +
	    	                              " data len: " + len +
	    	                              " checksum Len: " + checksumLen);
	    	 }
	    	 int checksumOff = buf.position(); //position at: |checksumdata|
	    	 int dataOff = checksumOff + checksumLen; //position at: |data|
	    	 byte pktBuf[] = buf.array();

		 buf.position(buf.limit()); //move to the end of the data
	    	 
	    	 // when client is writing the data, only the last namenode 
	    	 // needs to verify checksum for packet
	    	 if (mirrorOut == null ) {
	    		 //very packet with chunk checksum 
	    		 verifyChunks(pktBuf, dataOff, len, pktBuf, checksumOff); 
	    	 }
	    	 
	    	 byte[] lastChunkChecksum;
	    	 
	    	 try {
	    		 long onDiskLen = replicaInfo.getBytesOnDisk();
	    		 if (onDiskLen < offsetInFile) {
	    			 //onDiskLen - firstByteInFile should be zero
	    			int startByteToDisk = dataOff+(int)(onDiskLen-firstByteInFile);
	    	        int numBytesToDisk = (int)(offsetInFile-onDiskLen);
	    	        //write packet to disk
	    	        out.write(pktBuf, startByteToDisk, numBytesToDisk);
	    	        // get last checksum bytes
	    	        lastChunkChecksum = Arrays.copyOfRange(
	    	              pktBuf, checksumOff + checksumLen - checksumSize, 
	    	              checksumOff + checksumLen
	    	         );
	    	         checksumOut.write(pktBuf, checksumOff, checksumLen);
	    	         /// flush entire packet
	    	        flush();
	    			 
	    	        //set bytesondisk, lastchunkchecksum
	    	        replicaInfo.setLastChecksumAndDataLen(
	    	                offsetInFile, lastChunkChecksum);
	    		 }	 
	    	 } catch (IOException iex) {
	    		 throw iex;
	    	 }
	    }
	    //LOG.info("bufposition:" + buf.position() + ", remaining:" + 
	    //	buf.remaining() + ", limit:" + buf.limit() + ", islastpacket:"
	    //	+ lastPacketInFile);
	    return lastPacketInFile?-1:len;
	}
	
	/**
	  * Returns handles to the block file and its metadata file
	  */
	public synchronized BlockInputStreams getTmpInputStreams(ReplicaPoolFile rpf, 
			long blkOffset, long ckoff) throws IOException {
		ReplicaPoolInfo info = server.map.get(rpf.getFilename());
		File blockFile = info.getTempFile();
		RandomAccessFile blockInFile = new RandomAccessFile(blockFile, "r");
		if (blkOffset > 0) {
			blockInFile.seek(blkOffset);
		}
		File metaFile = info.getTempMetaFile();
	    RandomAccessFile metaInFile = new RandomAccessFile(metaFile, "r");
	    if (ckoff > 0) {
	      metaInFile.seek(ckoff);
	    }
	    return new BlockInputStreams(new FileInputStream(blockInFile.getFD()),
	                                new FileInputStream(metaInFile.getFD()));
	 
		
	}
	
	/**
	  * Returns the size of the header
	  */
	int getHeaderSize() {
		return Short.SIZE/Byte.SIZE + DataChecksum.getChecksumHeaderSize();
	}
	
	/**
	  * Flush block data and metadata files to disk.
	  * @throws IOException
	  */
	void flush() throws IOException {
		if (checksumOut != null) {
			checksumOut.flush();
	    }
	    if (out != null) {
	      out.flush();
	    }
	}
	
	
	/**
	  * Adjust the file pointer in the local meta file so that the last checksum
	  * will be overwritten.
	  */
	private void adjustCrcFilePosition() throws IOException {
		if (out != null) {
			out.flush();
		}
		if (checksumOut != null){
			checksumOut.flush();
		}
		FileOutputStream file = (FileOutputStream)streams.getChecksumOut();
		FileChannel channel = file.getChannel();
		long oldPos = channel.position();
		long newPos = oldPos - checksumSize;
		LOG.info("Changing meta file offset of file " + rpf + " from " + 
		        oldPos + " to " + newPos);
		channel.position(newPos);		
	}
	
	
	
	/**
	  * Reads (at least) one packet and returns the packet length.
	  * buf.position() points to the start of the packet and 
	  * buf.limit() point to the end of the packet. There could 
	  * be more data from next packet in buf.<br><br>
	  * 
	  * It tries to read a full packet with single read call.
	  * Consecutive packets are usually of the same length.
	  */
	private void readNextPacket() throws IOException {
	    if (buf == null ){
		LOG.info("create new buf to read packet");
		int chunkSize = bytesPerChecksum + checksumSize;
		int chunksPerPacket = (server.wirtePacketSize - server.PKT_HEADER_LEN -
				SIZE_OF_INTEGER + chunkSize -1)/chunkSize;
		buf = ByteBuffer.allocate(server.PKT_HEADER_LEN + SIZE_OF_INTEGER + 
				Math.max(chunksPerPacket, 1) * chunkSize);
		buf.limit(0);
	    }
	    //See if there is data left in the buffer
	    if (bufRead > buf.limit()) {
            	buf.limit(bufRead);
	    }
	    LOG.info("bufread:" + bufRead + ", buf position:" + buf.position()
			+ ", buf limit:" + buf.limit() + 
			", remaining:" + buf.remaining() + 
			", capacity:" + buf.capacity());
		
	    while (buf.remaining() < SIZE_OF_INTEGER) {
	        if (buf.position() > 0) {
	          shiftBufData();
	        }
	        readToBuf(-1);
	      }
		
	    //we mostly have the full packet or at least enough for an int
	    buf.mark();
	    int payloadLen = buf.getInt();
	    buf.reset();
	    
	    // check corrupt values for pktLen, 100MB upper limit should be ok?
	    if (payloadLen < 0 || payloadLen > (100*1024*1024)) {
	    	throw new IOException("Incorrect value for packet payload : " +
	                            payloadLen);
	    }
	    
	    int pktSize = payloadLen + server.PKT_HEADER_LEN;
	    LOG.info("buf.remaining:" + buf.remaining() + "==" + 
		"payloadlen:" + payloadLen + " + header:" + server.PKT_HEADER_LEN + " ?");
	    if (buf.remaining() < pktSize) {
	        LOG.info("need read more");
		//we need to read more data
	        int toRead = pktSize - buf.remaining();
	        
	        // first make sure buf has enough space.        
	        int spaceLeft = buf.capacity() - buf.limit();
	        if (toRead > spaceLeft && buf.position() > 0) {
	          shiftBufData();
	          spaceLeft = buf.capacity() - buf.limit();
	        }
	        if (toRead > spaceLeft) {
	          byte oldBuf[] = buf.array();
	          int toCopy = buf.limit();
	          buf = ByteBuffer.allocate(toCopy + toRead);
	          System.arraycopy(oldBuf, 0, buf.array(), 0, toCopy);
	          buf.limit(toCopy);
	        }
	        
	        //now loop read 
	        while (toRead > 0) {
	          toRead -= readToBuf(toRead);
	        }
	      }
	    
	    if (buf.remaining() > pktSize) {
	    	buf.limit(buf.position() + pktSize);
	    }
	    if (pktSize > maxPacketReadLen) {
	    	maxPacketReadLen = pktSize;
	    }	
	}
	
	
	/**
	  * Makes sure buf.position() is zero without modifying buf.remaining().
	  * It moves the data if position needs to be changed.
	  */
	private void shiftBufData() {
		if (bufRead != buf.limit()) {
	    throw new IllegalStateException("bufRead should be same as " +
	                                    "buf.limit()");
	    }
	    
	    //shift the remaining data on buf to the front
	    if (buf.position() > 0) {
	    	int dataLeft = buf.remaining();
	    	if (dataLeft > 0) {
	    		byte[] b = buf.array();
	    		System.arraycopy(b, buf.position(), b, 0, dataLeft);
	    	}
	    	buf.position(0);
	    	bufRead = dataLeft;
	    	buf.limit(bufRead);
	    }
	}
	
	/**
	  * reads upto toRead byte to buf at buf.limit() and increments the limit.
	  * throws an IOException if read does not succeed.
	  */
	private int readToBuf(int toRead) throws IOException {
	    if (toRead < 0) {
	    	toRead = (maxPacketReadLen > 0 ? maxPacketReadLen : buf.capacity())
	              - buf.limit();
	    }
	   
	    int nRead = in.read(buf.array(), buf.limit(), toRead);
	    
	    if (nRead < 0) {
	    	throw new EOFException("while trying to read " + toRead + " bytes");
	    }
	    bufRead = buf.limit() + nRead;
	    buf.limit(bufRead);
	    LOG.info("read to buf, buf position:" + buf.position() +
			" buf limit:" + buf.limit() + ", toread:" + toRead + 
			", nread:" + nRead); 
	    return nRead;
	}
	
	/**
	  * While writing to mirrorOut, failure to write to mirror should not
	  * affect this namenode unless it is caused by interruption.
	  */
	private void handleMirrorOutError(IOException ioe) throws IOException {
		LOG.info(server + ":Exception writing block " +
	             rpf + " to mirror " + mirrorAddr + "\n" +
	             StringUtils.stringifyException(ioe));
	    if (Thread.interrupted()) { // shut down if the thread is interrupted
	      throw ioe;
	    } else { // encounter an error while writing to mirror
	      // continue to run even if can not write to mirror
	      // notify client of the error
	      // and wait for the client to shut down the pipeline
	      mirrorError = true;
	    }
	}
	
	/**
	   * Verify multiple CRC chunks. 
	   */
	private void verifyChunks( byte[] dataBuf, int dataOff, int len, 
	                           byte[] checksumBuf, int checksumOff ) 
	                           throws IOException {
	    LOG.info("verify chunks");
	    while (len > 0) {
	      int chunkLen = Math.min(len, bytesPerChecksum);
	      checksum.update(dataBuf, dataOff, chunkLen);

	      if (!checksum.compare(checksumBuf, checksumOff)) {
	    	LOG.info("report corrupt file" + rpf + "from namenode " +
	    			NameNode.client.clientName);
	    	//FIXME
	    	//reportBadFile
	        throw new IOException("Unexpected checksum mismatch " + 
	                              "while writing " + rpf + " from " + inAddr);
	      }

	      checksum.reset();
	      dataOff += chunkLen;
	      checksumOff += checksumSize;
	      len -= chunkLen;
	    }
	  }
		
	/**
	  * Processed responses from downstream namenodes in the pipeline
	  * and sends back replies to the originator.
	  */
	class PacketResponder implements Runnable, FSConstants {
		
		//packet waiting for ack
		private LinkedList<Packet> ackQueue = new LinkedList<Packet>();
		private volatile boolean running = true;
		private ReplicaPoolFile rpf;
		DataInputStream mirrorIn; //input from downstream(next) namenode
		DataOutputStream replyOut; // output for upstream namenode
		private int numTargets;
		private FileReceiver receiver; //the owner of this responder.
		private Thread receiverThread; // the thread that spawns this responder
		
		public String toString() {
			return "PacketResponder " + numTargets + " from file " + this.rpf; 
		}
		
		PacketResponder(FileReceiver receiver, ReplicaPoolFile rpf,
				DataInputStream in, DataOutputStream out, int numTargets,
				Thread receiverThread) {
			this.receiverThread = receiverThread;
			this.receiver = receiver;
			this.rpf = rpf;
			mirrorIn = in;
			replyOut = out;
			this.numTargets = numTargets;
		}
		
	    /**
	     * enqueue the seqno that is still be to acked by the downstream datanode.
	     * @param seqno
	     * @param lastPacketInFile
	     * @param lastByteInFile
	     */
		synchronized void enqueue(long seqno, boolean lastPacketInFile, 
				long lastByteInPacket) {
			if (running){
				LOG.info("Packet Responder enqueue, numtargets " + numTargets + "adding seqno" +
						seqno + " to ack queue.");
				ackQueue.addLast(new Packet(seqno, lastPacketInFile, lastByteInPacket));
				notifyAll();
			}
		}
		
		/**
	     * wait for all pending packets to be acked. Then shutdown thread.
	     */
		synchronized void close() {
			while (running && ackQueue.size() != 0 && server.shouldRun) {
				try {
					wait();
				} catch (InterruptedException e) {
					running = false;
				}
			}
			LOG.debug("PacketResponder " + numTargets +
					" for file " + this.rpf + "Closing down");
			running = false;
			notifyAll();
		}
		
	    /**
	     * Thread to process incoming acks.
	     * @see java.lang.Runnable#run()
	     */
		public void run() {
			boolean lastPacketInFile = false;
			final long startTime = System.nanoTime();
			LOG.info("start packet responder thread, running:" + running 
				+ ", server shouldrun:" + server.shouldRun + ", lastpacket:"
				+ lastPacketInFile);
			while (running && server.shouldRun && !lastPacketInFile) {
				
				boolean isInterrupted = false;
				try {
					Packet pkt = null;
					long expected = -2;
					PipelineAck ack = new PipelineAck();
					long seqno = PipelineAck.UNKOWN_SEQNO;
					try {
						if (numTargets != 0 && !mirrorError) {
							//not the last NN & no mirror error
							//read an ack from downstream namenode: |seqno|num|replies|
							ack.readFields(mirrorIn);
							seqno = ack.getSeqno();
						}
						if (seqno != PipelineAck.UNKOWN_SEQNO || numTargets == 0){
							synchronized (this) {
								while (running && server.shouldRun && ackQueue.size() == 0){
									wait();
								}
								if (!running || !server.shouldRun) {
									break;
								}
								pkt = ackQueue.getFirst();
								expected = pkt.seqno;
								if (numTargets > 0 && seqno != expected) {
									throw new IOException("PacketResponder " + numTargets +
											" for file " + rpf + 
											" expected seqno: " + expected +
											" received: " + seqno);									
								}
								lastPacketInFile = pkt.lastPacketInFile;					
							}
						}
					} catch (InterruptedException ine) {
						isInterrupted = true;						
					} catch (IOException ioe) {
						if (Thread.interrupted()) {
							isInterrupted = true;
						} else {
							// continue to run even if can not read from mirror
							// notify client of the error and wait for client to shutdown pipeline
							mirrorError = true;
							LOG.info("PacketResponder " + rpf + " " + numTargets + 
									" Exception " + StringUtils.stringifyException(ioe));
						}
					}
					if (Thread.interrupted() || isInterrupted) {
						// The receiver thread cancelled this thread
					    LOG.info("PacketResponder " + rpf +  " " + numTargets +
	                    		" : Thread is interrupted.");
					    running = false;
					    continue;
					}
					
		            // If this is the last packet in file, then close 
		            // file and finalize the file before responding success
					if (lastPacketInFile) {
						receiver.close();
						final long endTime = System.nanoTime();
						rpf.setNumBytes(replicaInfo.getNumBytes());
						finalizeFile();
					}
					
					//construct my ack message
					Status[] replies = null;
					if (mirrorError) { //ack read error
						replies = new Status[2];
						replies[0] = SUCCESS;
						replies[1] = ERROR;
					} else {
						short ackLen = numTargets == 0 ? 0 : ack.getNumOfReplies();
						replies = new Status[1+ackLen];
						replies[0] = SUCCESS;
						for (int i=0; i<ackLen; i++) {
							replies[i+1] = ack.getReply(i);
						}
					}
					PipelineAck replyAck = new PipelineAck(expected, replies);
					
					//send my ack back to upstream namenode
					LOG.info("send packet" + expected + " ack to upstream node");
					replyAck.write(replyOut);
					replyOut.flush();
					if(LOG.isDebugEnabled()) {
						LOG.debug("PacketResponder " + numTargets + 
								" for file " + rpf + "responded an ack: " + replyAck);
					}
					if (pkt != null) {
						// remove the packet from the ack queue
						removeAckHead();
						// update bytes acked
						if (replyAck.isSuccess() &&
								pkt.lastByteInFile > replicaInfo.getBytesAcked()) {
							replicaInfo.setBytesAcked(pkt.lastByteInFile);
						}
					}
				} catch (IOException e) {
					LOG.warn("IOException in FileReceiver.run(): ", e);
					if (running) {
						LOG.info("PacketResponder " + rpf + " " + numTargets + 
								" Exception " + StringUtils.stringifyException(e));
						running = false;
						if (!Thread.interrupted()) { //failure not caused by interruption
							receiverThread.interrupt();
						}
					}
				} catch (Throwable e) {
					if (running) {
						LOG.info("PacketResponder " + rpf + " " + numTargets + 
								"Exception " + StringUtils.stringifyException(e));
						running = false;
						receiverThread.interrupt();
					}
				}			
			}
			LOG.info("PacketResponder, numtargets: " + numTargets + 
					" for file " + rpf + " terminating");
		}
		
		
	    /**
	     * Remove a packet from the head of the ack queue
	     * 
	     * This should be called only when the ack queue is not empty
	     */
		private synchronized void removeAckHead() {
			ackQueue.removeFirst();
			notifyAll();
		}
		
		
	}
	
	/**
	 * Complete the file write!
	 */
	public synchronized void finalizeFile() throws IOException {
		ReplicaPoolInfo replicaInfo = server.map.get(rpf.getFilename());
		if (replicaInfo.getState() == ReplicaState.FINALIZED) {
			// this is legal, when recovery happens on a file that has
		    // been opened for append but never modified
			return;
		}
		FinalizedPoolReplica newReplicaInfo = null;
		if (replicaInfo.getState() == ReplicaState.RUR &&
				((ReplicaPoolUnderRecovery)replicaInfo).getOriginalReplicaState() == 
				 ReplicaState.FINALIZED) {
			newReplicaInfo = (FinalizedPoolReplica)
					((ReplicaPoolUnderRecovery)replicaInfo).getOriginalReplica();
		} else {
			File src = replicaInfo.getTempFile();
			File srcmeta = replicaInfo.getTempMetaFile();
			File dst = replicaInfo.getFinalFile();
			File dstmeta = replicaInfo.getFinalMetaFile();
			LOG.info("finalize, src:" + src.getPath() + ", srcmeta:" + srcmeta.getPath()
				+ ", dst:" + dst.getPath() + ", dstmeta:" + dstmeta.getPath());
			if (src == null || srcmeta == null) {
				throw new IOException("No file for temporary file " + src + 
						" for file" + replicaInfo);
			}
			if (!srcmeta.renameTo(dstmeta) ||
				!src.renameTo(dst)) {
				throw new IOException("could not move files for" + rpf
						+ " from " + src + " to " + dst.getAbsolutePath()
						+ " or from " + srcmeta + "to " + dstmeta);
			}
			newReplicaInfo = new FinalizedPoolReplica(replicaInfo, dst.getParentFile());
		}
		server.map.put(replicaInfo.getFilename(), newReplicaInfo);
	}
	
	   
	/**
	 * close files.
	 */
	public void close() throws IOException {
		IOException ioe = null;
		
		 // close checksum file
	    try {
	      if (checksumOut != null) {
	        checksumOut.flush();
	        checksumOut.close();
	        checksumOut = null;
	      }
	    } catch(IOException e) {
	      ioe = e;
	    }
	    // close block file
	    try {
	      if (out != null) {
	        out.flush();
	        out.close();
	        out = null;
	      }
	    } catch (IOException e) {
	      ioe = e;
	    }
	    // disk check
	    if(ioe != null) {
	    	throw ioe;
	    }
	}
	
	/**
	  * This information is cached by the Datanode in the ackQueue.
	  */
	static private class Packet {
		long seqno;
	    boolean lastPacketInFile;
	    long lastByteInFile;

	    Packet(long seqno, boolean lastPacketInFile, long lastByteInPacket) {
	    	this.seqno = seqno;
	    	this.lastPacketInFile = lastPacketInFile;
	    	this.lastByteInFile = lastByteInPacket;
	    }
	}
	
	
}
