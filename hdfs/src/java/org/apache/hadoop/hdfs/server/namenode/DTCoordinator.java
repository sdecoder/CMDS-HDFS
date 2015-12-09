package org.apache.hadoop.hdfs.server.namenode;


import java.util.*;
import java.util.concurrent.*;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys.FSOperations;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.FSConstants; 
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

public class DTCoordinator {
	int message_counter = 0;
	private boolean is_aborted = false;
 	ConcurrentHashMap<Integer, FSOperations> msgslots = new ConcurrentHashMap<Integer, FSOperations>();
	String uuid;
  	NameNode namenode = null;
 
	public void setNamenode(NameNode parameter) {
		this.namenode = parameter;

	}

	public void setUUID(String uuid) {
		this.uuid = uuid;
	}
 
	 
	public void setCohortList(Integer[] cohortidx) {
		for (Integer integer: cohortidx) {
			msgslots.put(integer, FSOperations.UNKNOWN);
		}
  	} 
	
	public Set<Integer> getCohortList() {
		synchronized (msgslots) {
			return msgslots.keySet();
		}
	}

	public void process_cohort_msg(Integer hostid, FSOperations opcode) {
		
		if (opcode == FSOperations.PREPARE_COMMIT) {
		
		}else if(opcode == FSOperations.PREPARE_ABORT){
			is_aborted = true;
		
		}else {
		
		}
  		msgslots.put(hostid, opcode);
  		message_counter ++;
	 
	}

	public boolean is_ready() { 
		if (message_counter == msgslots.keySet().size()) {
			return true;
		}
		return false;
	}
	
	public boolean is_aborted() { 
		return is_aborted; 
	}
	
	public boolean getFinalStatusForTransaction() {
		if (is_aborted) {
			return false;
		}
		return true;
	}

}
