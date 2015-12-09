package org.apache.hadoop.hdfs.server.common;

import static org.apache.hadoop.hdfs.server.common.PoolDataTransferProtocol.Status.SUCCESS;
import static org.apache.hadoop.hdfs.server.common.PoolDataTransferProtocol.Status.ERROR;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSInputChecker;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.security.BlockAccessToken;
import org.apache.hadoop.hdfs.security.InvalidAccessTokenException;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.DataChecksum;

/** This is a wrapper around connection to namenode
 * and understands checksum, offset etc
 */
public class FileReader extends InputStream{
	
	Socket nnSock; //for now just sending checksumOk.
	private DataInputStream in;
	private DataChecksum checksum;
	private long startOffset;
	
	private final long firstChunkOffset;
	private long lastChunkOffset = -1;
	private long lastChunkLen = -1;
	private int bytesPerChecksum;
	private int checksumSize;
	
	private ByteBuffer checksumBuf;	//checksum data buffer
	private IntBuffer checksumInts; //wrapper on checksum buffer
	private ByteBuffer packetBuf; //packet data buffer
	private int packetSize; //packet size
	private int count = 0; //buf count
	private int countRead = 0;   // have read in packetBuf
	private int dataPos = 0;     // last packet pos in data stream
	private int checksumPos = 0; // pos in checksum buffer
	private long dataRead = 0; 	 // have read data
	private static long fileLen = 0;		// file len
	
	
	public static FileReader newFileReader(Socket sock, String rootpath, 
			String filename, long startOffset, String clientName) 
	throws IOException {
		//send read file command
		PoolDataTransferProtocol.Sender.opReadFile(
			new DataOutputStream(new BufferedOutputStream(
		            NetUtils.getOutputStream(sock,HdfsConstants.WRITE_TIMEOUT))),
		    rootpath, filename, 0, clientName);
		
		DataInputStream in = new DataInputStream(
				new BufferedInputStream(NetUtils.getInputStream(sock), 
						NameNode.client.STREAM_BUFFER_SIZE_DEFAULT));
		//get ack
		PoolDataTransferProtocol.Status status = PoolDataTransferProtocol.Status.read(in);

		if (status != SUCCESS) {
			throw new IOException("Got error for OP_READ_FILE, self =" 
					+ sock.getLocalSocketAddress() + ", remote="
					+ sock.getRemoteSocketAddress() + ", for file" + filename);
		}
		
		//|type|bytesperchecksum|offset|filelen|
		DataChecksum checksum = DataChecksum.newDataChecksum(in);
		long firstChunkOffset = in.readLong(); //default 0
		fileLen = in.readLong();
		
		return new FileReader(rootpath, filename, in, checksum, 
						startOffset, firstChunkOffset, sock);		
	}
	
	private FileReader(String rootpath, String filename, DataInputStream in, 
			DataChecksum checksum, long startOffset, long firstChunkOffset, 
			Socket nnSock) {
		this.nnSock = nnSock;
		this.in = in;
		this.checksum = checksum;
		this.startOffset = Math.max(startOffset, 0);
		
		//FIXME bytesNeededToFinish = bytesToRead + (startOffset - firstChunkOffset);
		this.firstChunkOffset = firstChunkOffset;
		lastChunkOffset = firstChunkOffset;
		lastChunkLen = -1;
		
		bytesPerChecksum = this.checksum.getBytesPerChecksum();
		checksumSize = this.checksum.getChecksumSize();	
		packetSize = NameNode.client.packetSize;
	}
	
	/** kind of like readFully(). Only reads as much as possible.
	  * And allows use of protected readFully().
	  */
	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		if (count == 0) {
			// Read next packet if there is no data in buffer
			//packet format |4B(packetLen)|8B(offset)|8B(seqno)|
			//|1B(islastPacket)|4B(dataLen)|checksum|data|
			int packetLen = in.readInt();
			long offsetInFile = in.readLong();
			long seqno = in.readLong();
			boolean lastpacketInFile = in.readBoolean();
			int dataLen = in.readInt();
			//if is the last packet, receive null packet and return end of file
			if (dataLen == 0) {
				if (!lastpacketInFile) {
					throw new IOException("Excepted empty end-of-read packet! Header: " +
							"(packetLen : " + packetLen + ", offsetInFile : " + offsetInFile +
							", seqno : " + seqno);
				}
				return -1;
			}
			
			adjustChecksumBytes(dataLen);
			IOUtils.readFully(in, checksumBuf.array(), 0, checksumBuf.limit());	
			int chunks = (dataLen + bytesPerChecksum -1) / bytesPerChecksum;
			if (dataLen > 0 && len > packetSize) {
				//read full packet
				IOUtils.readFully(in, b, off, dataLen); //packet size
				verifyChecksum(b, off, dataLen);
				return dataLen;
			} 
			//pre-read newt whole packet
			adjustDataBytes();
			//copy packet to packet buffer for lazy-checksum
			IOUtils.readFully(in, packetBuf.array(), 0, packetBuf.limit()); 
			System.arraycopy(packetBuf.array(), 0, b, off, len);
			packetBuf.position(len);
			count += len;
			return len;
		} else {
			//read from packet buffer
			int nread = Math.min(len, packetBuf.remaining());
			System.arraycopy(packetBuf, packetBuf.position(), b, off, nread);
			countRead += nread;
			packetBuf.position(countRead);
			if (packetBuf.remaining() == 0) {
				//do checksum
				verifyChecksum(packetBuf.array(), 0, packetBuf.limit());
				count = 0;
			}
			return nread;	
		}	
	}
		
	/**
	 * read one byte from inputstream
	 */
	@Override
	public int read() throws IOException {
		byte[] oneByteBuffer = new byte[1];
		int ret = read(oneByteBuffer, 0, 1);
		return (ret <= 0) ? -1 : (oneByteBuffer[0] & 0xff);
	}
	
	
		
	//checksum each chunk in the packet
	public void verifyChecksum(byte[] b, int off, int read) 
	throws IOException {
			int len = (int)Math.min(packetSize, read);
			len = ((len + bytesPerChecksum -1)/bytesPerChecksum) *4;//checksum size is 4
			checksumInts = checksumBuf.asIntBuffer();
			checksumInts.rewind();
			checksumInts.limit((read -1)/bytesPerChecksum + 1);
			
			int leftToVerify = read;
			int verifyOff = 0;
			while (leftToVerify > 0) {
				checksum.update(b, off + verifyOff, Math.min(leftToVerify, bytesPerChecksum));
				int expected = checksumInts.get();
				int calculated = (int)checksum.getValue();
				checksum.reset();
				
				if (expected != calculated) {
					long errPos = off + verifyOff;
					throw new ChecksumException("Checksum error from remote node: ", errPos);			
				}
				leftToVerify -= bytesPerChecksum;
				verifyOff += bytesPerChecksum;
			}
			dataPos +=read;
	}
	
	/**
	  * Makes sure that checksumBytes has enough capacity 
	  * and limit is set to the number of checksum bytes needed 
	  * to be read.
	  */
	private void adjustChecksumBytes(int dataLen) {
		int requiredSize = 
	      ((dataLen + bytesPerChecksum - 1)/bytesPerChecksum)*checksumSize;
	    if (checksumBuf == null || requiredSize > checksumBuf.capacity()) {
	      checksumBuf =  ByteBuffer.wrap(new byte[requiredSize]);
	    } else {
	      checksumBuf.clear();
	    }
	    checksumBuf.limit(requiredSize);
	    
	  }
	
	/**
	  * Makes sure that packetBuf has enough capacity 
	  * and limit is set to the number of bytes needed 
	  * to be read.
	  */
	private void adjustDataBytes() {
		int requiredSize = (int)Math.min(packetSize, fileLen - dataPos);
	    if (packetBuf == null || requiredSize > packetBuf.capacity()) {
	    	 packetBuf =  ByteBuffer.wrap(new byte[requiredSize]);
	    } else {
	    	packetBuf.clear();
	    }
	    countRead = 0;
	    packetBuf.limit(requiredSize);
	  }
	
	@Override
	public synchronized void close() throws IOException {
		checksum = null;
		if (nnSock != null) {
			IOUtils.closeSocket(nnSock);
			nnSock = null;
		}
		if (in != null) {
			IOUtils.closeStream(in);
			in = null;
		}
	}
	
}
