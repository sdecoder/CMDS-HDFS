package org.apache.hadoop.hdfs.server.common;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.SocketOutputStream;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;

/**
 * Reads a file from the disk and sends it to a recipient.
 */
class FileSender implements java.io.Closeable, FSConstants {
	public static final Log LOG = LogFactory.getLog(FileSender.class);
		
	private File file; //the file to read from
	private File metafile;  //checksum file to read from
	private InputStream fileIn; //data stream
	private DataInputStream checksumIn; //checksum stream
	private DataChecksum checksum;	//checksum
	private int bytesPerChecksum;	//chunk size
	private int checksumSize; 	//checksum size =4
	private long offset;  // starting position to read
	private long endOffset; //end position
	private String client; //client name
	private PoolServer node;
	private long seqno; // sequence number of packet
	private long fileInPosition = -1; // updated while using transferTo().
	
	//data file sender
	FileSender(String rootpath, String filename, long startOffset, 
			PoolServer node, String client) throws IOException {
		try {
			this.file = new File(rootpath, filename);
			this.metafile = new File(rootpath, filename + ".meta");
			this.endOffset = file.length();
			this.client = client;
			this.node = node;
			
			if (!file.exists() || metafile.exists()) {
				throw new IOException("file or metafile is unavailable");
			}
			//get checksum inputstream
			checksumIn = new DataInputStream(
		            	   new BufferedInputStream(
		            	     new FileInputStream(metafile), BUFFER_SIZE));
			BlockMetadataHeader header = 
				BlockMetadataHeader.readHeader(checksumIn);
			short version = header.getVersion();
			if (version != FSDataset.METADATA_VERSION) {
				LOG.info("Wrong version (" + version + ") ignoring ...");
			}
			checksum = header.getChecksum();
			bytesPerChecksum = checksum.getBytesPerChecksum();
			checksumSize = checksum.getChecksumSize();
			//FIXME seek to the right offsets
			//offset = (startOffset - (startOffset % bytesPerChecksum));
			offset = startOffset; //default 0
			seqno = 0;
			
			RandomAccessFile randomFile = new RandomAccessFile(file, "r");
			if (offset > 0) {
				randomFile.seek(offset);
			}
			fileIn = new FileInputStream(randomFile.getFD());
		} catch (IOException ioe) {
			IOUtils.closeStream(this);
			IOUtils.closeStream(checksumIn);
			IOUtils.closeStream(fileIn);
			throw ioe;
		}
	}	
		
	/**
	 * sendFile() is used to read file and its checksum and stream 
	 * the data to client.
	 * @param out stream to which the file is written to
	 * @param baseStream optional. if non-null, <code>out</code> is assumed to 
     *	 	  be a wrapper over this stream. This enables optimizations for
     *        sending the data, e.g. 
     *        {@link SocketOutputStream#transferToFully(FileChannel, 
     *        long, int)}.
     * @return total bytes reads, including crc.
	 */
	long sendFile(DataOutputStream out,OutputStream baseStream)
	throws IOException {
		if (out == null) {
			throw new IOException("out stream is null");
		}
		
		long initialOfffset = offset;
		long totalRead  = 0;
		OutputStream streamForSendChunks = out;
		
		final long startTime = System.nanoTime();
		try{
			try {
				//|type|bytesperchecksum|offset|filelen|
				checksum.writeHeader(out);
				out.writeLong(offset);
				out.writeLong(file.length());
				out.flush();
			} catch (IOException e) { //socket error
				throw new SocketException("Original Exception : " + e);
			}
			
			int maxChunksPerPacket;
			int pktSize = DataNode.PKT_HEADER_LEN + SIZE_OF_INTEGER;
			FileChannel filechannel = ((FileInputStream)fileIn).getChannel();
			fileInPosition = filechannel.position();
			streamForSendChunks = baseStream;
			
			//assure a mininum buffer size
			maxChunksPerPacket = (NameNode.client.packetSize + 
					bytesPerChecksum -1)/bytesPerChecksum;
			pktSize += checksumSize * maxChunksPerPacket;
			//only store checksum
			ByteBuffer pktBuf = ByteBuffer.allocate(pktSize);
			
			//send file data
			while (endOffset > offset) {
				long len = sendChunks(pktBuf, maxChunksPerPacket, 
									streamForSendChunks);
				offset += len;
				totalRead += len + ((len+bytesPerChecksum-1)/bytesPerChecksum *
									checksumSize);
				seqno++;
			}
			try {
				//send an empty packet to mark the end of file
				sendChunks(pktBuf, maxChunksPerPacket, streamForSendChunks);
				out.flush();
			} catch (IOException e) {
				throw new SocketException("Original exception : " + e);
			}
		} finally {
			LOG.info("fininsh sending file");	
			close();
		}
		return totalRead;
	}
	
	/**
	  * Sends upto maxChunks chunks of data.
	  * 
	  * When fileInPosition is >= 0, assumes 'out' is a 
	  * {@link SocketOutputStream} and tries 
	  * {@link SocketOutputStream#transferToFully(FileChannel, long, int)} to
	  * send data (and updates blockInPosition).
	  */
	private int sendChunks(ByteBuffer pkt, int maxChunks, OutputStream out)
	throws IOException {
		// Sends multiple chunks in one packet with a single write().
		
		int len = Math.min((int)(endOffset - offset), 
				bytesPerChecksum*maxChunks);
		int numChunks = (len + bytesPerChecksum -1)/bytesPerChecksum;
		//packetlen = |dataLen|checksum|data|
		int packetLen = len + numChunks * checksumSize +4;
		pkt.clear();
		
		//write packet header
		//packet format |4B(packetLen)|8B(offset)|8B(seqno)|
		//|1B(islastPacket)|4B(dataLen)|checksum|data|
		pkt.putInt(packetLen);
		pkt.putLong(offset);
		pkt.putLong(seqno);
		pkt.put((byte)((len == 0) ? 1:0));
		pkt.putInt(len);
		
		//fill checksum
		int checksumOff = pkt.position();
		int checksumLen = numChunks * checksumSize;
		byte[] buf = pkt.array();		
		if(checksumSize > 0 && checksumIn != null) {
			try {
				checksumIn.readFully(buf, checksumOff, checksumLen);
			} catch (IOException e) {
				IOUtils.closeStream(checksumIn);
				throw e;
			}
		}
		//write packet
		int dataOff = checksumOff + checksumLen;
		try {
			//use transferTo() to zero-copy
			SocketOutputStream sockOut = (SocketOutputStream)out;
			//first write the checksum
			sockOut.write(buf, 0, dataOff);
			sockOut.transferToFully(((FileInputStream)fileIn).getChannel(), 
					fileInPosition, len);
			fileInPosition += len;
		} catch (IOException e) {
			//exception while writing to the client
			throw new SocketException("Original Exception : " + e);
		}
		return len;
	}
	
	
	/**
	 * close opened files.
	 */
	public void close() throws IOException {
		IOException ioe = null;
		//close checksum file
		if(checksumIn != null) {
			try {
				checksumIn.close();
			} catch (IOException e) {
				ioe = e;
			}
			checksumIn = null;
		}
		
		if(fileIn != null) {
			try {
				fileIn.close();
			} catch (IOException e) {
				ioe = e;
			}
			fileIn = null;
		}
		// throw IOException if there is any
		if (ioe != null) {
			throw ioe;
		}
	}
	
}