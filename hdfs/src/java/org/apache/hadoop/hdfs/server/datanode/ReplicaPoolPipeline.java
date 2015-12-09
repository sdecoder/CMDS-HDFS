package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.hadoop.hdfs.server.common.HdfsConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.FSDatasetInterface.BlockWriteStreams;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DataChecksum;

/** 
 * This class defines a replica in a pipeline, which
 * includes a persistent replica being written to by a 
 * source namenode or being copied for the balancing purpose.
 * The base class implements a temporary replica
 */

public class ReplicaPoolPipeline extends ReplicaPoolInfo
								 implements ReplicaInPipelineInterface {
	
	public static final Log LOG = LogFactory.getLog(ReplicaPoolPipeline.class);
	private long bytesAcked;
	private long bytesOnDisk;
	private byte[] lastChecksum;
	private Thread writer;
		
	/**
     * Constructor for a zero length replica
     * @param root path ${root}
     * @param filename file name
     * @param genStamp replica generation stamp
     * @param dir directory path where files are located
     */
	public ReplicaPoolPipeline(String rootpath, String filename,
			File dir) {
		this (rootpath, filename, 0L, dir, Thread.currentThread());
	}
	
	/**
     * Constructor for a zero length replica
     * @param file image or edit file
     * @param len file length
     * @param genStamp replica generation stamp
     * @param state replica state
     */
	ReplicaPoolPipeline(ReplicaPoolFile rpf, File dir, Thread writer) {
		this (rpf.getRootpath(), rpf.getFilename(), rpf.getNumBytes(),
				dir, writer);
	}
	
	/**
	  * Constructor
	  * @param file image or edit file
	  * @param tmpfile temporary file for pipeline
	  * @len file length
	  * @genStamp replica generation stamp
	  * @param writer a thread that is writing to this replica
	  */
	ReplicaPoolPipeline(String rootpath, String filename, long len,
			File dir, Thread writer) {
		super(rootpath, filename, len, dir);
		this.bytesAcked = len;
		this.bytesOnDisk = len;
		this.writer = writer;
	}
	
	/**
	 * Copy constructor.
	 * @param from
	 */
	ReplicaPoolPipeline(ReplicaPoolPipeline from) {
		super(from);
		this.bytesAcked = from.getBytesAcked();
		this.bytesOnDisk = from.getBytesOnDisk();
		this.writer = from.writer;
	}
	
	@Override
	public long getVisibleLength() {
	    return -1;
	}
	
	@Override  //ReplicaInfo
	public ReplicaState getState() {
		return ReplicaState.TEMPORARY;
	}
	
	@Override //ReplicaInPipelineInterface
	public long getBytesAcked() {
		return bytesAcked;
	}
	
	@Override //ReplicaInPipelineInterface
	public void setBytesAcked(long bytesAcked) {
		this.bytesAcked = bytesAcked;
	}
	
	@Override //ReplicaInPipelineInterface
	public long getBytesOnDisk() {
		return bytesOnDisk;
	}
	
   @Override // ReplicaInPipelineInterface
   public synchronized void setLastChecksumAndDataLen(long dataLength, byte[] lastChecksum) {
	   this.bytesOnDisk = dataLength;
	   this.lastChecksum = lastChecksum;
   }
	  
   @Override // ReplicaInPipelineInterface
   public synchronized ChunkChecksum getLastChecksumAndDataLen() {
	   return new ChunkChecksum(getBytesOnDisk(), lastChecksum);
   }
	
   /**
    * Set the thread that is writing to this replica
    * @param writer a thread writing to this replica
    */
   public void setWriter(Thread writer) {
	   this.writer = writer;
   }
	
   @Override //Object
   public boolean equals(Object o) {
	   return super.equals(o);
   }
   
   /**
    * Interrupt the writing thread and wait until it dies
    * @throws IOException the waiting is interrupted
    */
   void stopWriter() throws IOException {
	   if (writer != null && writer != Thread.currentThread() && writer.isAlive()) {
		   writer.interrupt();
		   try {
			   writer.join();
		   } catch (InterruptedException e) {
			   throw new IOException("Waiting for writer thread is interrupted.");
		   }
	   }
   }
   
   /**
    * Create output streams for writing to this replica, 
    * one for file and one for CRC buffer
    * 
    * @param isCreate if it is for creation
    * @param bytePerChunk number of bytes per CRC chunk
    * @param checksumSize number of bytes per checksum
    * @return output streams for writing
    * @throws IOException if any error occurs
    */   
   @Override //ReplicaInPipelineInterface
   public BlockWriteStreams createStreams(boolean isCreate, 
		   int bytesPerChunk, int checksumSize) throws IOException {
	   
	   File file= getTempFile();
	   File metaFile = getTempMetaFile();
	   long fileDiskSize = 0L;
	   long crcDiskSize = 0L;
	   if (!isCreate) { //check on disk file
		   //FIXME
		   /* 
		   fileDiskSize = bytesOnDisk;		
		   //checksum .meta, |2B(version)|1B(type)|4B(bytesperchecksum)|
		   crcDiskSize = getHeaderSize() + 
		   		(fileDiskSize+bytesPerChunk-1)/bytesPerChunk*checksumSize;
		   */
	   }
	   FileOutputStream fileOut = null;
	   FileOutputStream crcOut = null;
	   try {
		   fileOut = new FileOutputStream(
				   new RandomAccessFile(file, "rw").getFD() );
		   crcOut = new FileOutputStream(
				   new RandomAccessFile(metaFile, "rw").getFD());
		   LOG.info("create stream, file:" + file.getPath() + ", metafile:" 
			+ metaFile.getPath());
		   if (!isCreate) {
			   fileOut.getChannel().position(fileDiskSize);
			   crcOut.getChannel().position(crcDiskSize);
		   }
		   return new BlockWriteStreams(fileOut, crcOut);
	   } catch (IOException e) {
		   IOUtils.closeStream(fileOut);
		   IOUtils.closeStream(crcOut);
		   throw e;
	   }	   
   }
   
   /**
    * Returns the size of the header
    */
   int getHeaderSize() {
     return Short.SIZE/Byte.SIZE + DataChecksum.getChecksumHeaderSize();
   }
   
   /** get generation stamp */
   @Override
   public long getGenerationStamp() {
	   return 0;
   }
   
   
   @Override
   public String toString() {
	   return getClass().getSimpleName() 
	   	+ "\n bytesAcked=" + bytesAcked
	   	+ "\n bytesOnDisk=" + bytesOnDisk;
   }
  
   
}
