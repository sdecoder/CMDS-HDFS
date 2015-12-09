package org.apache.hadoop.hdfs.server.common;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.File;
import java.util.List;
import java.util.zip.Checksum;
import java.util.ArrayList;


import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.commons.logging.Log;
import org.apache.hadoop.hdfs.server.common.PoolFile.NameNodeInfo;
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;

/********************************************************
 * override write function for pool storage
 *
 ********************************************************/

public class PoolFileOutputStream extends FileOutputStream {
	public static final Log LOG = LogFactory.getLog(PoolFileOutputStream.class);
	protected Configuration conf = new Configuration();		
	//the staticstics for this PoolFileInputStream
	protected Statistics statistics;
	
	//data checksum
	private Checksum sum;
	// internal buffer for storing data before it is checksumed
	private byte buf[];
	// internal buffer for storing checksum
	private byte checksum[];
	// The number of valid bytes in the buffer.
	private int count;
	private boolean isLastPacket = false;
	//bytes perchecksum
	final int bytesPerChecksum = NameNode.client.bytesPerChecksum;
	//socket read timeout
	final int socketTimeout = NameNode.client.socketTimeout;
	
	//pipeline outputstream
	PipelineOutputStream pipeStream = null;
				
	protected PoolFile pFile;
	protected DataOutputStream out = null;  //local file outputstream
	protected DataOutputStream metaout = null; //local checksum outputstream
	protected List<NameNodeInfo> nodes = new ArrayList<NameNodeInfo>(); //pipeline nodes
	public PoolFileOutputStream (File f) throws IOException {
		super(((PoolFile)f).getLocalFile());
		pFile = (PoolFile)f;
		File file = new File(pFile.getRootpath(), pFile.getName());
		File metafile = new File(pFile.getRootpath(), pFile.getName() + ".meta");
		//construct dirmap for metafile
		PoolFile m = new PoolFile(new File(pFile.getRootpath()), pFile.getName() + ".meta");
		LOG.info("clear meta nodeinfos and reset");
		m.nodeInfos.clear();
		m.nodeInfos = pFile.getNodeInfos();
		
		LOG.info("enter poolfile outputstream file: " + file.getPath());
		LOG.info("enter poolfile outputstream metafile: " + metafile.getPath());
		out = new DataOutputStream(
				new BufferedOutputStream(
						new FileOutputStream(file)));
		metaout = new DataOutputStream(
				new BufferedOutputStream(
						new FileOutputStream(metafile)));
		//write metaout header
		DataChecksum sum = DataChecksum.newDataChecksum(DataChecksum.CHECKSUM_CRC32,
			bytesPerChecksum);
		BlockMetadataHeader.writeHeader(metaout, sum);

		for (NameNodeInfo node : pFile.getNodeInfos()) {
			if (NameNode.client.clientName != node.getHostName()) {
				//remove local node
				LOG.info("add pipeline node: " + node.getHostName());
				nodes.add(node);
			}
		}
		this.sum = new PureJavaCrc32();
		this.buf = new byte[bytesPerChecksum];
		this.checksum = new byte[4];
		this.count = 0;		
		this.isLastPacket = false;
		this.pipeStream = new PipelineOutputStream(conf, socketTimeout, 
				pFile.getRootpath(),pFile.getName(), bytesPerChecksum, 
				NameNode.client.packetSize, nodes.toArray(new NameNodeInfo[nodes.size()]));
		if (this.pipeStream == null) {
			LOG.info("pipeStream not initialized");
			throw new IOException();
		}
	}
	
	@Override
	// write buffer b, generate a checksum for each data chunk
	public void write(byte[] b) throws IOException {
		LOG.info("enter write byte[]");
		int len = b.length;
		for (int n=0;n<len;n+=write1(b, n, len-n)) {
		}
	}
	
	@Override
	//write buffer b from offset off, generate a checksum fro each chunk
	public void write(byte[] b, int off, int len) throws IOException {
		LOG.info("enter write byte[], offset:" + off + ", len:" + len);
		if (off <0 || len <0 || off > b.length -len) {
			throw new ArrayIndexOutOfBoundsException();
		}
		
		for (int n=0;n<len;n+=write1(b, off+n, len-n));
	}
	
	@Override
	// Write one byte
	public void write(int b) throws IOException {
		LOG.info("enter write byte: " + b);
		sum.update(b);
		buf[count++] = (byte)b;
		if (count == buf.length) {
			flushBuffer();
		}	
	}
	
	/**
	  * Write a portion of an array, flushing to the underlying
	  * stream at most once if necessary.
	  */
	private int write1(byte b[], int off, int len) 
	throws IOException {
		if (count ==0 && len >= buf.length) {
			//write one chunk
			final int length = buf.length;
			sum.update(b, off, length);
			writeChecksumChunk(b, off, length, false);
			return length;
		}
		// copy user data to local buffer
		int bytesToCopy = buf.length - count;
		bytesToCopy = (len<bytesToCopy) ? len : bytesToCopy;
		sum.update(b, off, bytesToCopy);
		System.arraycopy(b, off, buf, count, bytesToCopy);
		count += bytesToCopy;
		if (count == buf.length) {
			flushBuffer();
		}
		return bytesToCopy;
	} 
	
	
	@Override 
	public void close() throws IOException {

		//flush remain bytes in packet
		LOG.info("enter close and set islastpacket: true");
		isLastPacket = true;
		flushBuffer(false);
		pipeStream.joinStreamer();
	}
	
		
	/* Forces any buffered output bytes to be checksumed and written out to
	 * the underlying output stream. 
	 */
	protected synchronized void flushBuffer() throws IOException {
	    flushBuffer(false);
	}
	
	/* Forces any buffered output bytes to be checksumed and written out to
	 * the underlying output stream.  If keep is true, then the state of 
	 * this object remains intact.
	 */
	protected synchronized void flushBuffer(boolean keep) throws IOException {
		if (count != 0 ) {
			int chunkLen = count;
			count =0;
			writeChecksumChunk(buf, 0, chunkLen, keep);
			if (keep) {
				count = chunkLen;
			}
		} else if(count ==0 && isLastPacket) {
			//all data has been written when close
			byte[] temp = new byte[0];
			pipeStream.writeChunk(temp, 0, 0, checksum, nodes, isLastPacket);
		}
	}
	
	private void writeChecksumChunk(byte b[], int off, int len, boolean keep)
		throws IOException {
		int tempChecksum = (int)sum.getValue();
		if (!keep) {
			sum.reset();
		}
		int2byte(tempChecksum, checksum);
		LOG.info("enter write checksum chunk, first local, off:" + 
			off + ", len:" + len + ", checksum:" + tempChecksum);
		//first write local
		out.write(b, off, len);
		//then write meta file
		metaout.write(checksum, 0, 4);
		if (isLastPacket) {
			LOG.info("isLastPacket: " + isLastPacket + ", flush and close local file");
			out.close();
			metaout.close();
		}

		LOG.info("then pipestream writechunk, isLastPacket: " + isLastPacket);
		pipeStream.writeChunk(b, off, len, checksum, nodes, isLastPacket);
		
	}
	
	static byte[] int2byte(int integer, byte[] bytes) {
		bytes[0] = (byte)((integer >>> 24) & 0xFF);
		bytes[1] = (byte)((integer >>> 16) & 0xFF);
		bytes[2] = (byte)((integer >>>  8) & 0xFF);
		bytes[3] = (byte)((integer >>>  0) & 0xFF);
		return bytes;
	}
	
			
	
}
