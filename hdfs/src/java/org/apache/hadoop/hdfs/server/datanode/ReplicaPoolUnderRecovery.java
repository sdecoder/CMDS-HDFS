package org.apache.hadoop.hdfs.server.datanode;

import java.io.File;

import org.apache.hadoop.hdfs.server.common.HdfsConstants.ReplicaState;

/**
 * This class represents replicas that are under file recovery
 * It has a recovery id that is equal to the generation stamp 
 * that the replica will be bumped to after recovery
 * The recovery id is used to handle multiple concurrent file recoveries.
 * A recovery with higher recovery id preempts recoveries with a lower id.
 *
 */

public class ReplicaPoolUnderRecovery extends ReplicaPoolInfo {
	private ReplicaPoolInfo original; // the original replica that needs to be recovered
	private long recoveryId; // recovery id; it is also the generation stamp 
     						 // that the replica will be bumped to after recovery

	ReplicaPoolUnderRecovery(ReplicaPoolInfo replica, long recoveryId) {
		super(replica.getRootpath(), replica.getFilename(), 
				replica.getNumBytes(), replica.getDir());
	    if ( replica.getState() != ReplicaState.FINALIZED &&
	            replica.getState() != ReplicaState.RBW &&
	            replica.getState() != ReplicaState.RWR ) {
	         throw new IllegalArgumentException("Cannot recover replica: " + replica);
	       }
	    this.original = replica;
	    this.recoveryId = recoveryId;
	}
	
	/**
	 * Copy constructor.
	 */
	ReplicaPoolUnderRecovery(ReplicaPoolUnderRecovery from) {
		super(from);
		this.original = from.original;
		this.recoveryId = from.getRecoveryID();
	}
	
	/** 
	  * Get the recovery id
	  * @return the generation stamp that the replica will be bumped to 
	  */
	long getRecoveryID() {
		return recoveryId;
	}
	
	/**
	 * Set the recovery id
	 * @param recoveryId the new recoveryId
	 */
	void setRecoveryID(long recoveryId) {
		if (recoveryId > this.recoveryId) {
			this.recoveryId = recoveryId;
		} else {
			throw new IllegalArgumentException("The new rcovery id: " + recoveryId
		            + " must be greater than the current one: " + this.recoveryId);
		}
	}
	
	/**
	 * Get the original replica that's under recovery
	 * @return the original replica under recovery
	 */
	public ReplicaPoolInfo getOriginalReplica(){
		return original;
	}
	
	/**
	 * Get the original replica's state
	 * @return the original replica's state
	 */
	public ReplicaState getOriginalReplicaState() {
		return original.getState();
	}
	
	@Override //ReplicaInfo
	boolean isUnlinked() {
		return original.isUnlinked();
	}
	
	@Override //ReplicaInfo
	void setUnlinked() {
		original.setUnlinked();
	}
	
	@Override //ReplicaInfo
	public ReplicaState getState() {
		return ReplicaState.RUR;
	}
	
	@Override
	public long getVisibleLength() {
		return original.getVisibleLength();
	}
	
	@Override
	public long getBytesOnDisk() {
		return original.getBytesOnDisk();
	}
	
	@Override 
	public void setNumBytes(long numBytes) {
		super.setNumBytes(numBytes);
	    original.setNumBytes(numBytes);
	}
	
	@Override //ReplicaInfo
	void setDir(File dir) {
		super.setDir(dir);
	    original.setDir(dir);
	}
	
	@Override  // Object
	public boolean equals(Object o) {
		return super.equals(o);
	}
	
	@Override
	public String toString() {
	    return super.toString()
	        + "\n  recoveryId=" + recoveryId
	        + "\n  original=" + original;
	}
	
	ReplicaPoolRecoveryInfo createInfo() {
		return new ReplicaPoolRecoveryInfo(original.getRootpath(),
				original.getFilename(), original.getBytesOnDisk(),
				getOriginalReplicaState());
	}
		
}