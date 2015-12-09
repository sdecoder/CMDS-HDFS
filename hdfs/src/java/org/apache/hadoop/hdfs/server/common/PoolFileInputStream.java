package org.apache.hadoop.hdfs.server.common;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.File;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.List;
import java.util.ArrayList;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.common.PoolFile.NameNodeInfo;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.commons.logging.Log;


/********************************************************
 * override write function for pool storage
 *
 ********************************************************/

public class PoolFileInputStream extends FileInputStream {
	public static final Log LOG = LogFactory.getLog(PoolFileInputStream.class);
		
	//the staticstics for this PoolFileInputStream
	private PoolFile pFile;
	private FileInputStream fileIn = null;  //data stream
	private DataInputStream checksumIn = null; //checksum datatream
	private List<NameNodeInfo> nodes = new ArrayList<NameNodeInfo>();
	private byte[] checksumBuf; //checksum data buffer
	private IntBuffer checksumInts; //wrapper on checksum buffer
	private ByteBuffer packetBuf; 	//packet data buffer
	private int count = 0; //buf count
	private DataChecksum checksum;
	private long fileLen; //file length
	private int packetSize; //packet size
	private int dataPos = 0;     // last packet pos in data stream
	private long dataRead = 0; 	 // have read data
	private final int bytesPerChecksum = NameNode.client.bytesPerChecksum;
	private byte[] oneByteBuff = new byte[1]; //used for 'int read()'
	private FileReader reader = null;	
	Socket nn = null; //socket for read data
	// Number of checksum chunks that can be read at once into a user
	// buffer. Chosen by benchmarks - higher values do not reduce cpu usage.
	private static final int CHUNKS_PER_READ = 32;
	
	public PoolFileInputStream (File f) throws IOException {
		super(((PoolFile)f).getLocalFile());
		pFile = (PoolFile)f;
		File file = new File(pFile.getRootpath(), pFile.getName());
		File metafile = new File(pFile.getRootpath(), pFile.getName() + ".meta");
		fileLen = file.length();
		packetSize = NameNode.client.packetSize;
		LOG.info("create poolfile inputstream, file:" + file.getPath() 
			+ ", meta file:" + metafile.getPath() + ", packetsize:"
			+ packetSize + ", filelen:" + fileLen);

		//checksum size is 4
		checksumBuf = new byte[(packetSize/bytesPerChecksum)*4];
		//buffer data to integrate an packet size
		packetBuf = ByteBuffer.allocate(packetSize);
		for (NameNodeInfo node: pFile.getNodeInfos()) {
			if (NameNode.client.clientName != node.getHostName()) {
				//remove local node
				nodes.add(node);
			}
		}
		
		if (file.exists() && metafile.exists()) {
			//open local file stream
			LOG.info("open local image and meta");
			fileIn = new FileInputStream(file);
			checksumIn = new DataInputStream(
					new BufferedInputStream(
							new FileInputStream(metafile))); 	
			//read header and checksum
			short version = checksumIn.readShort();
			checksum = DataChecksum.newDataChecksum(checksumIn);
			LOG.info("meta version:" + version + ", bytesperchecksum:" + 
				checksum.getBytesPerChecksum());
		} else {
			LOG.info("local file " + file + " corrupted, read from remote");
			if (nodes == null || nodes.size() == 0) {
				LOG.info("No node available for file" + file);
				throw new IOException();
			}
			NameNodeInfo target = nodes.get(0);
			InetSocketAddress targetAddr = NetUtils.createSocketAddr(target.getHostPortName());
			nn = new Socket();
			NetUtils.connect(nn, targetAddr, NameNode.client.socketTimeout);
			nn.setSoTimeout(NameNode.client.socketTimeout); //set read block time		
			
			reader = FileReader.newFileReader(nn, pFile.getRootpath(), 
					pFile.getName(), 0, NameNode.client.clientName);
			if (reader == null) {
				throw new NullPointerException();
			}
		}
	}
	
	//read one byte, firstly from local, or from remote nodes
	@Override
	public int read() throws IOException{
		if (fileIn!= null && checksumIn != null){
			//from local
			LOG.info("enter local read()");
			int ret = fileIn.read(oneByteBuff, 0, 1);
			packetBuf.put(oneByteBuff[0]);
			count ++;
			dataRead += ret;
			count += ret;
			if ( (count % packetSize) == 0 || dataRead == fileLen) {
				verifyChecksum(packetBuf.array(), 0, (int)Math.min(packetSize, fileLen-dataPos));					
				count = 0;
				packetBuf.clear();
			}
			return (ret <= 0) ? -1: (oneByteBuff[0] & 0xff);
		}else {
			//from remote
			return reader.read();		
		}			
	}
	
	//firstly from local, or from remote nodes
	public int read(byte[] b) throws IOException {
		int len  = b.length;
		return read(b, 0, len);	
	}
		
	//firstly from local, or from remote nodes
	public int read(byte[] b, int off, int len) throws IOException {
	    // parameter check
	    if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
	      throw new IndexOutOfBoundsException();
	    } else if (len == 0) {
	      return 0;
	    }
		
		if (fileIn!= null && checksumIn != null){
			//from local
			LOG.info("enter read(byte[], off:" + off + ", len:" 
				+ len + ")");
			int n =0;
			for(;;) {
				int nread = read1(b, off +n, len -n);
				if (nread <=0)
					return (n == 0) ? nread: n;
				n += nread;
				if (n >= len)
					return n;
			}
		}else {
			//from remote
			int n =0;
			try{
				for(;;) {
					int mread = reader.read(b, off +n, len -n);
					if (mread <=0)
						return (n == 0) ? mread: n;
					n += mread;
					if (n >= len)
						return n;
				}
				
			} catch (ChecksumException e) {
				LOG.info("read got a checksum exception for file " + pFile);
			} catch (IOException e) {
				throw new IOException("original IOException");
			} 
			return n;
		}		
	}
	
	/**
	 * read packet or len data from local file and check chunk checksum
	 */
	private int read1(byte b[], int off, int len) 
	throws IOException {
		int nread = 0;
		if (count ==0 && len > packetSize ) {
			//read one packet
			nread = fileIn.read(b, off, packetSize);
			dataRead += nread;
			verifyChecksum(b, off, packetSize);
			return packetSize;
		}
		//read and copy user data to local buffer
		int bytesToCopy = packetSize - count;
		bytesToCopy = (len < bytesToCopy) ? len : bytesToCopy;
		nread = fileIn.read(b, off, bytesToCopy);
		dataRead += nread;
		System.arraycopy(b, off, packetBuf.array(), count, bytesToCopy);
		count += bytesToCopy;
		if (count == packetSize || dataRead == fileLen) {
			verifyChecksum(packetBuf.array(), 0, (int)Math.min(fileLen - dataPos, packetSize));
			count =0;
			packetBuf.clear();
		}
		return bytesToCopy;
	}
	
	//checksum each chunk in the packet
	public void verifyChecksum(byte[] b, int off, int read) 
	throws IOException {
			LOG.info("verify checksum, byte[]:" + b + 
				", off:" + off + ", len:" + read);
			int len = (int)Math.min(packetSize, read);
			len = ((len + bytesPerChecksum -1)/bytesPerChecksum) *4;//checksum size is 4
			checksumIn.readFully(checksumBuf, 0, len);
			checksumInts = ByteBuffer.wrap(checksumBuf).asIntBuffer();
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
					throw new ChecksumException("Checksum error: " + pFile, errPos);			
				}
				leftToVerify -= bytesPerChecksum;
				verifyOff += bytesPerChecksum;
			}
			dataPos +=read;
	}
	
	//close all open streams
	public void close() throws IOException {
		LOG.info("enter close");
		IOException ioe = null;
		//close checksum file
		if (checksumIn != null){
			try {
				checksumIn.close();
			} catch (IOException e) {
				ioe = e;
			}
			checksumIn = null;
		}
 		//close data file
		if (fileIn != null) {
			try {
				fileIn.close();
			} catch (IOException e){
				ioe = e;
			}
			fileIn = null;
		}
		//close file reader
		if (reader != null) {
			IOUtils.closeStream(reader);
			reader = null;
		}
		//close socket
		if (nn != null) {
			IOUtils.closeSocket(nn);
			nn = null;
		}
		if (ioe!= null) {
			throw ioe;
		}
	}
	
}
