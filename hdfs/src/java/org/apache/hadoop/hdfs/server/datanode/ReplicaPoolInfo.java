package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;
import java.io.IOException;


/**
 * This class is used by namenodes to maintain meta data of its replicas.
 * It provides a general interface for meta information of a replica.
 */
abstract public class ReplicaPoolInfo extends ReplicaPoolFile implements Replica {
	  private File     dir;    // directory where files belong
	 	  
	  /**
	   * Constructor for a zero length replica
	   * @param rootpath ${root}
	   * @param filename file name
	   * @param genStamp replica generation stamp
	   * @param file directory path where files are located
	   */
	  ReplicaPoolInfo(String rootpath, String filename, File dir) {
	    this(rootpath, filename, 0L, dir);
	  }
	  
	  /**
	   * Constructor
	   * @param ReplicaPoolFile replica pool file
	   * @param dir directory path where block and meta files are located
	   */
	  ReplicaPoolInfo(ReplicaPoolFile file, File dir) {
	    this(file.getRootpath(), file.getFilename(), 
	    		file.getNumBytes(), dir);
	  }
	  
	  
	  /**
	   * Constructor
	   * @param filename file
	   * @param len replica length
	   * @param genStamp replica generation stamp
	   * @param dir directory path where block and meta files are located
	   */
	  ReplicaPoolInfo(String rootpath, String filename, long len, 
			  File dir) {
	    super(rootpath, filename, len);
		this.dir = dir;
	  }	
	  
	  /**
	   * Copy constructor.
	   * @param from
	   */
	  ReplicaPoolInfo(ReplicaPoolInfo from) {
	    this(from.getRootpath(), from.getFilename(), from.getNumBytes(),
	    		 from.getDir());  
	  }
	  
	  //get tmp file, such as ${root}/current/rbw/fsimage
	  public File getTempFile() {
		  return new File(getDir(), getName());
	  }
	  
	  //get tmp meta file, such as ${root}/current/rbw/fsimage.meta
	  public File getTempMetaFile() {
		  return new File(getDir(), getName() + ".meta");
	  }
	  
	  //get image file, such as ${root}/current/fsimage
	  public File getFinalFile() {
		  return new File(getRootpath(), getFilename());
	  }
	  
	  //get image meta file,such as ${root}/current/fsimage.meta
	  public File getFinalMetaFile() {
		  return new File(getRootpath(), getFilename() + ".meta");
	  }
	  	  
	  /**
	   * Return the parent directory path where this replica is located
	   * @return the parent directory path where this replica is located
	   */
	  File getDir() {
	    return dir;
	  }
	  
	  /**
	   * Set the parent directory where this replica is located
	   * @param dir the parent directory where the replica is located
	   */
	  void setDir(File dir) {
	    this.dir = dir;
	  }

	  /**
	   * check if this replica has already been unlinked.
	   * @return true if the replica has already been unlinked 
	   *         or no need to be detached; false otherwise
	   */
	  boolean isUnlinked() {
	    return true;                // no need to be unlinked
	  }
	  
	  /**
	   * set that this replica is unlinked
	   */
	  void setUnlinked() {
	    // no need to be unlinked
	  }
	
	  private void unlinkFile(File file) throws IOException {
		  
	  }
	  
	  /**
	   * Set this replica's generation stamp to be a newer one
	   * @param newGS new generation stamp
	   * @throws IOException is the new generation stamp is not greater than the current one
	   */
	  void setNewerGenerationStamp(long newGS) throws IOException {
	    long curGS = getGenerationStamp();
	    if (newGS <= curGS) {
	      throw new IOException("New generation stamp (" + newGS 
	          + ") must be greater than current one (" + curGS + ")");
	    }
	    //setGenerationStamp(newGS);
	  }
	  
	  /** get generation stamp */
	  public long getGenerationStamp() {
		  return 0;
	  }
	  
	  @Override  //Object
	  public String toString() {
	    return getClass().getSimpleName()
	        + ", " + super.toString()
	        + ", " + getState()
	        + "\n  getNumBytes()     = " + getNumBytes()
	        + "\n  getBytesOnDisk()  = " + getBytesOnDisk()
	        + "\n  getVisibleLength()= " + getVisibleLength()
	        + "\n  getFile()    = " + getFinalFile();
	  }	 
	  
	  @Override //replica
	  public long getBlockId() {return -1;}
	  
	  
}