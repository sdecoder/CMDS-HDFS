package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.FSDataset.FSVolume;

/**
 * This class describes a replica that has been finalized.
 */
public class FinalizedPoolReplica extends ReplicaPoolInfo {
	 private boolean unlinked;		//copy-on-write done for file
	
	  /**
	   * Constructor
	   * @param rootpath ${root}
	   * @param filename file name
	   * @param len replica length
	   * @param genStamp replica generation stamp
	   * @param dir directory path where block and meta files are located
	   */
	  FinalizedPoolReplica(String rootpath, String filename, 
	  			File dir) {
		super(rootpath, filename, dir);
	  }	
	
	  /**
	   * Constructor
	   * @param rpf pool file
	   * @param dir directory path where files are located
	   */
	  public FinalizedPoolReplica(ReplicaPoolFile rpf, File dir) {
	    super(rpf, dir);
	  }
	  
	  /**
	   * Copy constructor.
	   * @param from
	   */
	  FinalizedPoolReplica(FinalizedPoolReplica from) {
	    super(from);
	    this.unlinked = from.isUnlinked();
	  }
	  
	  @Override  // ReplicaPoolInfo
	  public ReplicaState getState() {
	    return ReplicaState.FINALIZED;
	  }
	
	  @Override // ReplicaInfo
	  boolean isUnlinked() {
	    return unlinked;
	  }
	  
	  @Override  // ReplicaInfo
	  void setUnlinked() {
	    unlinked = true;
	  }
	  
	  @Override
	  public long getVisibleLength() {
	    return getNumBytes();       // all bytes are visible
	  }
	  
	  @Override
	  public long getBytesOnDisk() {
	    return getNumBytes();
	  }
	  
	  @Override  // Object
	  public boolean equals(Object o) {
	    return super.equals(o);
	  }
	  
	  @Override
	  public String toString() {
	    return super.toString()
	        + "\n  unlinked=" + unlinked;
	  }
	
}
