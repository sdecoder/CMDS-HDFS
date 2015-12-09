package org.apache.hadoop.hdfs.server.datanode;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.ReplicaState;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**
 * Replica recovery information.
 */
public class ReplicaPoolRecoveryInfo extends ReplicaPoolFile {
	private ReplicaState originalState;
	
	public ReplicaPoolRecoveryInfo() {
		
	}
	
	public ReplicaPoolRecoveryInfo(String rootpath, String filename,
			long diskLen, ReplicaState rState) {
		set(rootpath, filename, diskLen);
		originalState = rState;
	}
	
	public ReplicaState getOriginalReplicaState() {
		return originalState;
	}
		
}