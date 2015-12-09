package org.apache.hadoop.hdfs.server.namenode;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
//hadoop import
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.server.namenode.NameNode.*;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSConfigKeys.*;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSClient; 
import org.apache.hadoop.fs.Options;




public class DTCohort {
	
 	NameNode namenode;
	enum Status {
		prepare, commit, abort
	}

	Status dtstatus;
	String uuid; // current transaction id;
	Integer coordinator_idx;
	int rename_file_dst_host_idx = -1; //only used in rename_file_src
	Lock critical_area_lock = new ReentrantLock();
    Condition cohort_inside_condition = critical_area_lock.newCondition();
 	
    
    FSEditLog editlog = null;
    FSDirectory fs_dir = null;
    ConcurrentSkipListMap<String, String> lock_table = null;
	final public static String super_group_string = "supergroup";
	
	public DTCohort() {
 		this.dtstatus = Status.prepare;
	}

	public void setTransactionID(String _para) {
		this.uuid = _para;
	}

	public void setLocalNameNode(NameNode namenode) {
		this.namenode = namenode;
		this.editlog = this.namenode.namesystem.getEditLog();
		this.fs_dir = this.namenode.namesystem.dir;
		this.lock_table = namenode.namesystem.flat_lock_table ;
	}
   
	public void set_coordinator_idx(Integer coordinator_idx) {
		this.coordinator_idx = coordinator_idx;
	}

	public void setDTAction(DFSConfigKeys.FSOperations op_status) {
		if (op_status == FSOperations.FINAL_COMMIT) {
			this.dtstatus = Status.commit;
		}else if(op_status == FSOperations.FINAL_ABORT){
			this.dtstatus = Status.abort;
		}else {
			 
		}
		 
	}
	
	
	//========================================================================================
	// error handling section
	Status query_storagepool_for_transaction_status() throws  IOException{
		// nslog.info("[inf] Querying the storage pool, NOT IMPLEMENTED YET...");
		// "commit" or "abort"
		if (!NameNode.enable_failure_handling) {
			throw new IOException("failure handling not enabled: following code should not be executed...");
		}
		 
 		while (true) {
			try {
				String finalStatus = namenode.namesystem.getFSImage().editLog.queryWalTrans(this.uuid);				
				if (finalStatus.equals(DFSConfigKeys.COMMIT_STRING)) {
					return Status.commit;
				}else if(finalStatus.equals(DFSConfigKeys.ABORT_STRING)){
					return Status.abort;
				}else {
					//namenode.namesystem.getFSImage().editLog.logWalEdit(this.uuid, DFSConfigKeys.ABORT_STRING);
					return Status.abort;
				}
				
				
			} catch (Exception e) {
				NameNode.LOG.error(StringUtils.stringifyException(e));
				try {
					synchronized (this) {
						this.wait(NameNode.unified_timeout);
					}
				} catch (Exception e2) {
					NameNode.LOG.error(StringUtils.stringifyException(e2));
					throw new IOException(e2);
				}
				
				
			}

		}

 	}

	public void dtop_mkdir_parent_commit(String src, FsPermission permission, long now ) throws IOException {
		
		synchronized (fs_dir) {			
			namenode.mkdirs_without_log(src, permission, true, now); 
		}
		
		final String parent_src_path = new Path(src).getParent().toString();
		this.release_lock(parent_src_path);
		//NameNode.LOG.info("[dbg] dtop_mkdir_parent_commit [end]");
 		
	}
	
	public void dtop_mkdir_parent_abort(String src) throws IOException {
		//NameNode.LOG.info("[dbg] dtop_mkdir_parent_abort [start]");
		final String parent_src_path = new Path(src).getParent().toString();
		this.release_lock(parent_src_path);
		//NameNode.LOG.info("[dbg] dtop_mkdir_parent_abort [end]");
    }
	
	 
	
	public boolean dtop_mkdir_parent(String src, FsPermission permission, boolean createParent) throws IOException, InterruptedException {		
		//NameNode.LOG.info("[dbg] dtcohort: dtop_mkdir_parent [start]: " );
		//NameNode.LOG.info("[dbg] src: " + src );
		//NameNode.LOG.info("[dbg] permission: " + permission );
		//NameNode.LOG.info("[dbg] createParent: " + createParent );
		
		boolean result = true;	 
 		final String parent_src_path = new Path(src).getParent().toString();
		
 		if (fs_dir.getFileInfo(parent_src_path, true) == null) {
			//parent not exist!
			result = false;
		} 
		
		if (result) {
			result = this.try_lock_no_prefix_checking(parent_src_path);
		}
		
		DTCohort.print_current_line();
		//time oracle
 		final long now = FSNamesystem.now();
		if (result) {
			//final INode[] src_inodes = fs_dir.getExistingPathINodes(src);	
  			//final String shortUserName = UserGroupInformation.getCurrentUser().getShortUserName();
			//final String userGroupNames[] = UserGroupInformation.getCurrentUser().getGroupNames();
 			//userGroupNames[0] = super_group_string; // XXX: this will be fixed in future 
			//NameNode.LOG.info("[dbg] dtcohort: dtop_mkdir_parent@line" + 
			//		(new Throwable().getStackTrace()[0]).getLineNumber());
			
			final PermissionStatus permission_status = new PermissionStatus(
					UserGroupInformation.getCurrentUser().getShortUserName(),
		         	UserGroupInformation.getCurrentUser().getGroupNames()[0], permission);
			
			src = fs_dir.normalizePath(src);
			//NameNode.LOG.info("[dbg] dtcohort: dtop_mkdir_parent@line" + 
			//		(new Throwable().getStackTrace()[0]).getLineNumber());
			//NameNode.LOG.info("[dbg] normalized src: " + src);
		    String[] names = INode.getPathNames(src);
		    
		    //NameNode.LOG.info("[dbg] dtcohort: dtop_mkdir_parent@line" + 
		    //		(new Throwable().getStackTrace()[0]).getLineNumber());
		    byte[][] components = INode.getPathComponents(names);
		    //NameNode.LOG.info("[dbg] dtcohort: dtop_mkdir_parent@line" + 
		    //		(new Throwable().getStackTrace()[0]).getLineNumber());
		    INode[] inodes = new INode[components.length];
		    //NameNode.LOG.info("[dbg] inodes.length: " + inodes.length);
		    //NameNode.LOG.info("[dbg] names.length: " + names.length);

		    fs_dir.rootDir.getExistingPathINodes(components, inodes, false);
		    //NameNode.LOG.info("[dbg] dtcohort: dtop_mkdir_parent@line" + 
		    //		(new Throwable().getStackTrace()[0]).getLineNumber());
		    StringBuilder pathbuilder = new StringBuilder();
			int i = 1;
			for (; i < inodes.length && inodes[i] != null; i++) {
				pathbuilder.append(Path.SEPARATOR + names[i]);
			}
			//NameNode.LOG.info("[dbg] dtcohort: dtop_mkdir_parent@line" + 
			//		(new Throwable().getStackTrace()[0]).getLineNumber());
			//NameNode.LOG.info("[dbg] pathbuilder is: " + pathbuilder.toString());
			// create directories beginning from the first null index
			for (; i < inodes.length; i++) {
				//NameNode.LOG.info("[dbg] dtcohort: dtop_mkdir_parent@line" + 
				//		(new Throwable().getStackTrace()[0]).getLineNumber());
				//final String dbg_info = ": names[" + i + "]: " + names[i];
				//NameNode.LOG.info("[dbg] dtcohort: dtop_mkdir_parent@line" + 
				//		(new Throwable().getStackTrace()[0]).getLineNumber() + dbg_info);
				pathbuilder.append(Path.SEPARATOR + names[i]);
				//NameNode.LOG.info("[dbg] pathbuilder is: " + pathbuilder.toString());
				try {
					String cur = pathbuilder.toString(); 
	  				editlog.log_mkdir_transaction_ext(this.uuid, 
	  						cur, now, now, permission_status);
				} catch (Exception e) {
					NameNode.LOG.error(StringUtils.stringifyException(e));
					throw new IOException(e);
				}
				
				//NameNode.LOG.info("[dbg] dtcohort: dtop_mkdir_parent@line" + 
				//		(new Throwable().getStackTrace()[0]).getLineNumber());
			}	
			//NameNode.LOG.info("[dbg] dtcohort: dtop_mkdir_parent@line" + 
			//		(new Throwable().getStackTrace()[0]).getLineNumber());
 			editlog.logSync();			
 			//NameNode.LOG.info("[dbg] dtcohort: logSync[done]: dtop_mkdir_parent@" + 
 			//	(new Throwable().getStackTrace()[0]).getLineNumber());
 		}	
		
		//NameNode.LOG.info("[dbg] DTCohort.dtop_mkdir_parent: native prepare return: " + result);
		
		if (result) {
			namenode.send_transaction_status_to_coordinator(
					FSOperations.PREPARE_COMMIT, 
					this.uuid, 
					coordinator_idx);
		}else {
			namenode.send_transaction_status_to_coordinator(
					FSOperations.PREPARE_ABORT, 
					this.uuid, 
					coordinator_idx);
		}
		
		//NameNode.LOG.info("[dbg] DTCohort.dtop_mkdir_parent: critical area [start]");
		critical_area_lock.lock();
        try {
        	if (this.dtstatus == Status.commit) {		
        		this.dtop_mkdir_parent_commit(src, permission, now);					 
    		} else if (this.dtstatus == Status.abort) {
    			this.dtop_mkdir_parent_abort(src);
    		} else {
    			
    			cohort_inside_condition.await(NameNode.unified_timeout, TimeUnit.MILLISECONDS); //timeout control!
    			//NameNode.LOG.info("[dbg] DTCohort.dtop_mkdir_parent: critical area [awaken]");
    			//NameNode.LOG.info("[dbg] transaction status: " + this.dtstatus);
    			if (this.dtstatus == Status.commit) {		
    				this.dtop_mkdir_parent_commit(src, permission, now);			 
    			} else if (this.dtstatus == Status.abort) {
    				this.dtop_mkdir_parent_abort(src);
    			}else {
    				this.dtstatus = this.query_storagepool_for_transaction_status();
    				if (this.dtstatus == Status.commit) {		
        				this.dtop_mkdir_parent_commit(src, permission, now);			 
        			} else if (this.dtstatus == Status.abort) {
        				this.dtop_mkdir_parent_abort(src);
        			}
    			}
    		}
  
         } finally {
        	critical_area_lock.unlock();
         }
		
        //NameNode.LOG.info("[dbg] dtcohort: dtop_mkdir_parent [end]");
		return result;
	}
	
	public void dtop_mkdir_child_commit(String src, FsPermission permission, long now) throws UnresolvedLinkException, InterruptedException, IOException {
 		synchronized (fs_dir) {
 			//NameNode.LOG.info("[dbg] dtop_mkdir_child_commit [start]");
 			DTCohort.print_current_line();
 			namenode.mkdirs_without_log(src, permission, true, now);
 			this.release_lock(src);
 			//NameNode.LOG.info("[dbg] dtop_mkdir_child_commit [end]");
 			DTCohort.print_current_line();
		}
		
		
	 
 	}
	
	public void dtop_mkdir_child_abort(String src) throws IOException {
		//NameNode.LOG.info("[dbg] dtop_mkdir_child_abort [start]");
		this.release_lock(src);
		DTCohort.print_current_line();
		//NameNode.LOG.info("[dbg] dtop_mkdir_child_abort [end]");
	}

	public boolean dtop_mkdir_child(String src, FsPermission permission, boolean createParent) throws org.apache.hadoop.fs.UnresolvedLinkException, InterruptedException, IOException {
		//NameNode.LOG.info("[dbg] dtcohort: dtop_mkdir_child [start]: " + src);
		boolean result = true;		
		if (result) {
			//not allowed to be deleted or renamed
			result = this.try_lock_no_prefix_checking(src);
			
		}
		//time oracle
		final long now = FSNamesystem.now();
		if (result) {
			final PermissionStatus permission_status = new PermissionStatus(
					UserGroupInformation.getCurrentUser().getShortUserName(),
					UserGroupInformation.getCurrentUser().getGroupNames()[0], permission);
			DTCohort.print_current_line();
			src = fs_dir.normalizePath(src);
		    final String[] names = INode.getPathNames(src);
		    final byte[][] components = INode.getPathComponents(names);
		    final INode[] inodes = new INode[components.length];
		    fs_dir.rootDir.getExistingPathINodes(components, inodes, false);
		    final StringBuilder pathbuilder = new StringBuilder();
			int i = 1;
			for (; i < inodes.length && inodes[i] != null; i++) {
				pathbuilder.append(Path.SEPARATOR + names[i]);
			}

			// create directories beginning from the first null index
			for (; i < inodes.length; i++) {
				pathbuilder.append(Path.SEPARATOR + names[i]);
				String cur = pathbuilder.toString();
  				editlog.log_mkdir_transaction_ext(this.uuid, 
  						cur, now, now, permission_status);
			}	
 			editlog.logSync();		
 			
		}

		DTCohort.print_current_line();
		//NameNode.LOG.info("[dbg] dtcohort: this.dtstatus: " + this.dtstatus);
		//NameNode.LOG.info("[dbg] dtcohort: dtop_mkdir_child@line" +  (new Throwable().getStackTrace()[0]).getLineNumber());
		//NameNode.LOG.info("[dbg] DTCohort.dtop_mkdir_child: current status: " + this.dtstatus);
		//NameNode.LOG.info("[dbg] synchronizing using lock: " + this.dtstatus);
		boolean communication_result = false;
		if (result) {
			communication_result = namenode.send_transaction_status_to_coordinator(
						FSOperations.PREPARE_COMMIT, 
						this.uuid, 
						coordinator_idx);
		}else {
			communication_result = namenode.send_transaction_status_to_coordinator(
						FSOperations.PREPARE_ABORT, 
						this.uuid, 
						coordinator_idx);
		}
		critical_area_lock.lock();
		//NameNode.LOG.info("[dbg] dtcohort: dtop_mkdir_child@line" +  (new Throwable().getStackTrace()[0]).getLineNumber());

        try {
        	if (this.dtstatus == Status.commit) {		
    			dtop_mkdir_child_commit(src, permission, now);			 
    		} else if (this.dtstatus == Status.abort) {
    			dtop_mkdir_child_abort(src);
    		} else {
    			
    			cohort_inside_condition.await(NameNode.unified_timeout, TimeUnit.MILLISECONDS); //timeout control!
    			//NameNode.LOG.info("[dbg] DTCohort.dtop_mkdir_child: critical area [awaken]");
    			//NameNode.LOG.info("[dbg] transaction status: " + this.dtstatus);
    			if (this.dtstatus == Status.commit) {		
    				dtop_mkdir_child_commit(src, permission, now);			 
    			} else if (this.dtstatus == Status.abort) {
    				dtop_mkdir_child_abort(src);
    			}else {
    				this.dtstatus = this.query_storagepool_for_transaction_status();
    				if (this.dtstatus == Status.commit) {		
        				this.dtop_mkdir_child_commit(src, permission, now);			 
        			} else if (this.dtstatus == Status.abort) {
        				this.dtop_mkdir_child_abort(src);
        			}
     			}
    		}
        	//NameNode.LOG.info("[dbg] dtcohort.dtop_mkdir_child: critical area ends...");
  
         } catch (Exception e) {
			NameNode.LOG.error(StringUtils.stringifyException(e));
         } finally {
        	critical_area_lock.unlock();
         }

        // NameNode.LOG.info("[dbg] dtcohort: dtop_mkdir_child [end]: " + result);
		return result;
	}
	 
	
	public void dtop_delete_commit(String src) throws IOException{
		//NameNode.LOG.info("[dbg] dfsclient.dtop_delete_commit [start]");
 		namenode.delete_without_log(src, true);
 		this.release_lock(src);
 		//NameNode.LOG.info("[dbg] dfsclient.dtop_delete_commit [end]");
	}
	
	public void dtop_delete_abort(String src) throws IOException {
		//NameNode.LOG.info("[dbg] dfsclient.dtop_delete_commit: [start]");
		this.release_lock(src);
		//NameNode.LOG.info("[dbg] dfsclient.dtop_delete_commit: [end]");

	}

	public boolean dtop_delete(String src, boolean recursive) throws IOException, InterruptedException {
		//NameNode.LOG.info("[dbg] dfsclient.dtop_delete: [start]");
		boolean result = true;
 		boolean exist = namenode.namesystem.dir.exists(src);
		if (!exist) {
 			result = true;
		} else {
			result = this.try_lock_prefix_checking(src);
			 
		}
		//NameNode.LOG.info("[dbg] DTCohort@line " +  (new Throwable().getStackTrace()[0]).getLineNumber());
		//NameNode.LOG.info("[dbg] dtop_delete->result: " + result);
		final long now = FSNamesystem.now();  
		if (result) {
 			namenode.getFSImage().getEditLog().log_delete_transcation_ext(this.uuid, src, now);
			namenode.getFSImage().getEditLog().logSync();
		}

		//NameNode.LOG.info("[dbg] dtcohort@line " +  (new Throwable().getStackTrace()[0]).getLineNumber());
		boolean communication_result = false;
		if (result) {
			communication_result = namenode.send_transaction_status_to_coordinator(
					FSOperations.PREPARE_COMMIT, this.uuid, coordinator_idx);
		}else {
			communication_result = namenode.send_transaction_status_to_coordinator(
					FSOperations.PREPARE_ABORT, this.uuid, coordinator_idx);
		}
		//NameNode.LOG.info("[dbg] dtcohort@line " +  (new Throwable().getStackTrace()[0]).getLineNumber());
		critical_area_lock.lock();
        try {
        	if (this.dtstatus == Status.commit) {		
        		this.dtop_delete_commit(src);						 
    		} else if (this.dtstatus == Status.abort) {
    			this.dtop_delete_abort(src);	
    		} else {
    			
    			//NameNode.LOG.info("[dbg] dtcohort.dtop_delete.dir: waiting for status:");
    			cohort_inside_condition.await(NameNode.unified_timeout, TimeUnit.MILLISECONDS); //timeout control!
    			//NameNode.LOG.info("[dbg] dtcohort.dtop_delete.dir: signal received...:");
    			
    			if (this.dtstatus == Status.commit) {		
    				this.dtop_delete_commit(src);	 	 
    			} else if (this.dtstatus == Status.abort) {
    				this.dtop_delete_abort(src);	
    			}else {
    				this.dtstatus = this.query_storagepool_for_transaction_status();
    				if (this.dtstatus == Status.commit) {		
    					this.dtop_delete_commit(src);	 
        			} else if (this.dtstatus == Status.abort) {
        				this.dtop_delete_abort(src);
        			}

    			}
    		}
  
         } finally {
        	critical_area_lock.unlock();
        }
        
        // NameNode.LOG.info("[dbg] dfsclient.dtop_delete: [end]: " + result);
		return result;

	}
	
	public void dtop_rename_file_src_commmit(String src) throws IOException {
		//NameNode.LOG.info("[dbg] dtcohort.dtop_rename_file_src_commmit: [start]");
		fs_dir.unlink_inodefile(src);
		this.release_lock(src);
		//NameNode.LOG.info("[dbg] dtcohort.dtop_rename_file_src_commmit: [end]");
	 
		

 	}
	
	public void dtop_rename_file_src_abort(String src) throws IOException {
		//NameNode.LOG.info("[dbg] dtcohort.dtop_rename_file_src_abort: [start]");
		this.release_lock(src);
		//NameNode.LOG.info("[dbg] dtcohort.dtop_rename_file_src_abort: [end]");
		 
	}


	public boolean dtop_rename_file_src(final String src, final String dst) throws IOException, InterruptedException {
		//NameNode.LOG.info("[dbg] dtcohort.dtop_rename_file_src: [start]");
		
		boolean result = true; 
		//NameNode.LOG.info("[dbg] dtop_rename_file_src phase 1: checking source inode ");
		INodeFile inodefile = this.namenode.namesystem.dir.getFileINode(src);
		
		//NameNode.LOG.info("[dbg] dtop_rename_file_src phase 2: locking path");
		if (result) {
 			result = this.try_lock_no_prefix_checking(src);
 		}
		
		//NameNode.LOG.info("[dbg] dtop_rename_file_src phase 3: " + result);
		if (result) {	 
			//NameNode.LOG.info("[dbg] beforce serialize_inodefile_into_dataoutputbuffer: [cohort-src] "); 
			DataOutputBuffer dob = new DataOutputBuffer(); 
			//dob.writeInt(FSOperations.SEND_INODEFILE.ordinal()); //opcode
			dob.writeBytes(this.uuid + DFSConfigKeys.new_line);
			dob.writeBytes(dst + DFSConfigKeys.new_line);
			inodefile.write(dob);
   			namenode.send_inodefile(this.uuid, this.rename_file_dst_host_idx , dob);
   			
   			//NameNode.LOG.info("[dbg] dtop_rename_file_src phase: log_unlink_inodefile_transaction_ext:"); 
   			editlog.log_unlink_inodefile_transaction_ext(this.uuid, src);
			editlog.logSync();
  			NameNode.LOG.info("[dbg] after sending inodefile databuffer to: " + this.rename_file_dst_host_idx);
 	
		} 
		
		boolean comm_result = false;
		if (result) {
			comm_result = namenode.send_transaction_status_to_coordinator(
					FSOperations.PREPARE_COMMIT, this.uuid, this.coordinator_idx);
			
		}else {
			comm_result = namenode.send_transaction_status_to_coordinator(
					FSOperations.PREPARE_ABORT, this.uuid, this.coordinator_idx);
		}
		critical_area_lock.lock();
		NameNode.LOG.info("[dbg] after sending transaction status [" + result + "] to: " + this.coordinator_idx); 
		
        try {
        	if (this.dtstatus == Status.commit) {		
        		this.dtop_rename_file_src_commmit(src);					 
    		} else if (this.dtstatus == Status.abort) {
    			this.dtop_rename_file_src_abort(src);
    		} else {
    			cohort_inside_condition.await(NameNode.unified_timeout, TimeUnit.MILLISECONDS); //timeout control!
    		
    			if (this.dtstatus == Status.commit) {		
    				this.dtop_rename_file_src_commmit(src); 	 
    			} else if (this.dtstatus == Status.abort) {
    				this.dtop_rename_file_src_abort(src);
    			}else {
    				this.dtstatus = this.query_storagepool_for_transaction_status();
    				if (this.dtstatus == Status.commit) {		
        				this.dtop_rename_file_src_commmit(src); 	 
        			} else if (this.dtstatus == Status.abort) {
        				this.dtop_rename_file_src_abort(src);
        			}
    			}
    		}
  
         } finally {
        	critical_area_lock.unlock();
        }
        
        //NameNode.LOG.info("[dbg] dtcohort.dtop_rename_file_src: [end]");
		return result;
	}
	
	//======================================================================================================================
	
	public void dtop_rename_file_dst_commit(String dst) throws IOException {
		//NameNode.LOG.info("[dbg] dtcohort.dtop_rename_file_dst_commit: [start]");
		namenode.namesystem.dir.add_inodefile(dst,
				this.inodefile_for_rename_file_dst,
				this.inodefile_for_rename_file_dst.diskspaceConsumed(), true);
 		this.release_lock(dst);
 		//NameNode.LOG.info("[dbg] dtop_rename_file_dst_commit: after addINodeFile");
 		//NameNode.LOG.info("[dbg] dtcohort.dtop_rename_file_dst_commit: [end]");

	}
	
	public void dtop_rename_file_dst_abort(String dst) {
		//NameNode.LOG.info("[dbg] dtcohort.dtop_rename_file_dst_abort: [start]");
 		this.release_lock(dst);
 		//NameNode.LOG.info("[dbg] dtcohort.dtop_rename_file_dst_abort: [end]");
 		 
	}

	

	public boolean dtop_rename_file_dst(final String src, final String dst) throws IOException, InterruptedException {
		//NameNode.LOG.info("[dbg] DTCohort.dtop_rename_file_dst: [start] ");
		//NameNode.LOG.info("[dbg] target file name: " + dst);
		boolean result = true;
		final String dst_parent_path = (new Path(dst)).getParent().toString();
		if (result) {
			//NameNode.LOG.info("[dbg] DTCohort.dtop_rename_file_dst: l622");
 			if (fs_dir.getFileInfo(dst_parent_path, true) == null) {
 				//NameNode.LOG.info("[dbg] DTCohort.dtop_rename_file_dst: dst_parent_path is null");
				result = false;
			}
			
 			//NameNode.LOG.info("[dbg] DTCohort.dtop_rename_file_dst: l629");
			
		}
		
		if (result) {
			//NameNode.LOG.info("[dbg] DTCohort.dtop_rename_file_dst: l633");	
			result = result && this.try_lock_no_prefix_checking(dst);
			
			
		}
		
		
		//NameNode.LOG.info("[dbg] DTCohort.dtop_rename_file_dst: sending transaction flag to coordinator...[start]");
		//NameNode.LOG.info("[dbg] DTCohort.dtop_rename_file_dst: sending transaction flag to coordinator...[end]");
		 
		boolean comm_result = false;
		if (result) {
			comm_result = namenode.send_transaction_status_to_coordinator(
						FSOperations.PREPARE_COMMIT,  this.uuid, this.coordinator_idx);
			
		}else {
			comm_result = namenode.send_transaction_status_to_coordinator(
						FSOperations.PREPARE_ABORT,  this.uuid, this.coordinator_idx);
		}
		critical_area_lock.lock();
		
        try {
        	if (this.dtstatus == Status.commit) {		
        		this.dtop_rename_file_dst_commit(dst);				 
    		} else if (this.dtstatus == Status.abort) {
    			this.dtop_rename_file_dst_abort(dst);
    		} else {
    			
    			cohort_inside_condition.await(NameNode.unified_timeout, TimeUnit.MILLISECONDS); //timeout control!
    			if (this.dtstatus == Status.commit) {		
    				this.dtop_rename_file_dst_commit(dst);	 
    			} else if (this.dtstatus == Status.abort) {
    				this.dtop_rename_file_dst_abort(dst);
    			}else {
    				this.dtstatus = this.query_storagepool_for_transaction_status();
    				if (this.dtstatus == Status.commit) {		
        				this.dtop_rename_file_dst_commit(dst);	 
        			} else if (this.dtstatus == Status.abort) {
        				this.dtop_rename_file_dst_abort(dst);
        			}

    			}
    		}
  
        } finally {
        	critical_area_lock.unlock();
        }
 
        //NameNode.LOG.info("[dbg] DTCohort.dtop_rename_file_dst: [end]");
 		return result;
	}
	
	
	//================================================================================================
	public void dtop_rename_dir_commit(final String src, final String dst, 
 			String dst_parent_path, FsPermission src_permission,   long now, boolean src_existing) throws IOException {
		//NameNode.LOG.info("[dbg] DTCohort.dtop_rename_dir_commit: [start]");
		boolean result = true;
		if (!src_existing) {
			// no source, do nothing
			return;
		}
		
		if (fs_dir.getFileInfo(dst_parent_path, true) == null) {
			result = namenode.mkdirs_without_log(dst_parent_path, src_permission, true, now);
 		}
		result = result && namenode.rename_without_log(src, dst); 
 		this.release_lock(src);
		this.release_lock(dst);
	  
		//NameNode.LOG.info("[dbg] DTCohort.dtop_rename_dir_commit: [end]");
		return;
	}

	public void dtop_rename_dir_abort(String src, String dst) throws IOException{
		//NameNode.LOG.info("[dbg] DTCohort.dtop_rename_dir_abort: [start]");
		if (fs_dir.getFileInfo(src, true) == null) {
			// no source, do nothing
			return;
		}
		
		this.release_lock(src);
		this.release_lock(dst);
		//NameNode.LOG.info("[dbg] DTCohort.dtop_rename_dir_abort: [end]");
		
	} 

	public boolean dtop_rename_dir(String src, String dst, FsPermission permission) throws IOException {
		//NameNode.LOG.info("[dbg] DTCohort.dtop_rename_dir: [start]");
		
		boolean result = true;
		boolean src_existing = true;
		
		//NameNode.LOG.info("[dbg] DTCohort.dtop_rename_dir: phase 1: checking path...");
		if (result) {
			 if (fs_dir.getFileInfo(src, true) == null) {
				// no parent dir, no operation, return true;
				 src_existing = false;
			}
		}
		
		//NameNode.LOG.info("[dbg] DTCohort.dtop_rename_dir: phase 2: locking path...");
		if (src_existing) {
			result = result && this.try_lock_prefix_checking(src); //<-delete
			result = result && this.try_lock_no_prefix_checking(dst); //<-mkdir
		}
  		
		String dst_parent_path = (new Path(dst)).getParent().toString();
		final long now = System.currentTimeMillis(); //time oracle
		
		if (result) {		
			if (!src_existing) {
				//do nothing
			}else {
				//src exists!
				
				if (fs_dir.getFileInfo(dst_parent_path, true) == null) {
					//logging: create the dst_parent_path 
					
					final PermissionStatus permission_status = new PermissionStatus(
							UserGroupInformation.getCurrentUser().getShortUserName(),
							UserGroupInformation.getCurrentUser().getGroupNames()[0], permission);
					dst_parent_path = fs_dir.normalizePath(dst_parent_path);
				    String[] names = INode.getPathNames(dst_parent_path);
				    byte[][] components = INode.getPathComponents(names);
				    INode[] inodes = new INode[components.length];
				    fs_dir.rootDir.getExistingPathINodes(components, inodes, false);
				    StringBuilder pathbuilder = new StringBuilder();
					int i = 1;
					for (; i < inodes.length && inodes[i] != null; i++) {
						pathbuilder.append(Path.SEPARATOR + names[i]);
					}

					// create directories beginning from the first null index
					for (; i < inodes.length; i++) {
						pathbuilder.append(Path.SEPARATOR + names[i]);
						String cur = pathbuilder.toString();
		  				editlog.log_mkdir_transaction_ext(this.uuid, 
		  						cur, now, now, permission_status);
					}	
					
				}
				
				//logging rename: 
				editlog.log_rename_transaction_ext(this.uuid, src, dst, now);
				editlog.logSync();		
			}			
		}	 
 			 
		if (result) {
			namenode.send_transaction_status_to_coordinator(
					FSOperations.PREPARE_COMMIT, this.uuid, coordinator_idx);
		}else {
			namenode.send_transaction_status_to_coordinator(
					FSOperations.PREPARE_ABORT, this.uuid, coordinator_idx);
			
		}
		critical_area_lock.lock();
  		
  		
        try {
        	if (this.dtstatus == Status.commit) {		
        		this.dtop_rename_dir_commit(src, dst, dst_parent_path, permission, now, src_existing );
        		
    		} else if (this.dtstatus == Status.abort) {
    			this.dtop_rename_dir_abort(src, dst);
    			result = false;
    			
    		} else {

    			cohort_inside_condition.await(NameNode.unified_timeout, TimeUnit.MILLISECONDS); //timeout control!
    			if (this.dtstatus == Status.commit) {		
    				this.dtop_rename_dir_commit(src, dst, dst_parent_path, permission, now, src_existing);
    			} else if (this.dtstatus == Status.abort) {
    				 this.dtop_rename_dir_abort(src, dst);
    				 result = false;
    				 
    			}else {
    				this.dtstatus = this.query_storagepool_for_transaction_status();
        			if (this.dtstatus == Status.commit) {		
        				 this.dtop_rename_dir_commit(src, dst, dst_parent_path, permission, now, src_existing);
        			} else if (this.dtstatus == Status.abort) {
        				 this.dtop_rename_dir_abort(src, dst);
        				 result = false;
        				 
        			}
    			
    			}
    		}
  
        } catch (Exception e) {
			throw new IOException(e);
		}finally {
        	critical_area_lock.unlock();
        }
       
        //  NameNode.LOG.info("[dbg] DTCohort.dtop_rename_dir: [end]");
		return result;
	}

	 
	
	INodeFile inodefile_for_rename_file_dst = null;
	public void restore_inodefile_from_dataoutputbuffer(String dst, DataInputBuffer dib) throws IOException {
		//NameNode.LOG.info("[dbg] DTCohort.restore_inodefile_from_dataoutputbuffer: [start]");
		try {
			
			//NameNode.LOG.info("[dbg] start to construct inodefile");
			this.inodefile_for_rename_file_dst = new INodeFile();
			this.inodefile_for_rename_file_dst.readFields(dib);
			namenode.namesystem.getEditLog().logAddINodeFileTransactionExt(
					this.uuid, 
					dst, 
					inodefile_for_rename_file_dst, 
					inodefile_for_rename_file_dst.diskspaceConsumed());
 			namenode.namesystem.getEditLog().logSync();
		} catch (Exception e) {
			//NameNode.LOG.info(StringUtils.stringifyException(e));
			throw new IOException(e);
		}
		
		//NameNode.LOG.info("[dbg] DTCohort.restore_inodefile_from_dataoutputbuffer: [end]");
 
	}
	
	//=============================================================================================
	// concurrent control section
	public SortedMap<String, String> filter_prefix(SortedMap<String, String> base_set, String prefix) {
		if (prefix.length() > 0) {
			if (!prefix.endsWith("/")) {
				prefix = prefix + "/";
			}
			char nextLetter = (char) (prefix.charAt(prefix.length() - 1) + 1);
			String end = prefix.substring(0, prefix.length() - 1) + nextLetter;
			return base_set.subMap(prefix, end);
		} else
			return null;

	}
	
	public boolean try_lock_no_prefix_checking(String src) {
		if (NameNode.enable_concurrent_control) {
			synchronized (lock_table) {
				if (lock_table.get(src) != null) {
 					return false;
				}
				lock_table.put(src, this.uuid);
 				return true;
			}
		}
		
		return true;
		
		
	}
	
	public boolean try_lock_prefix_checking(String prefix) {
		if (NameNode.enable_concurrent_control) {
			synchronized (lock_table) {
				if(!this.try_lock_no_prefix_checking(prefix)){
	 				return false;
				}
				
				SortedMap<String, String> sub_set = filter_prefix(lock_table, prefix);
				if (sub_set.size() > 0) {
					lock_table.remove(prefix);
 	 				return false;
				}

				//lock_table.put(prefix, this.uuid);
 				return true;
			}
		}
		
		return true;
		
		
	}
	

	public void release_lock(String src){
		if (NameNode.enable_concurrent_control) {
			synchronized (lock_table) {
				String trans_id = lock_table.get(src);
				if (trans_id == null) {
					return;
				}else if(trans_id.equals(this.uuid)) {
					lock_table.remove(src);	
				}else {
					return;
				}
				
				 
			}
 		}
		
		
	}
	//=============================================================================================
	
	public final static boolean is_inode_existing(INode[] nodes, int idx) {
		if (idx < 0) {
			return false;
		}
		
		if (nodes[idx] == null) {
			return false;
		}
		return true;
	}
	
	//coding
	List<String> candidate_dirs = null;
	public String[] get_candidate_dirs() {
		candidate_dirs = new ArrayList<String>();
		for (String path: candidate_dirs) {
			 synchronized (lock_table) {
				if (lock_table.get(path) != null) {
					//this path is locked by other transactions
 					candidate_dirs.remove(path);
					continue;
				}else {
					lock_table.put(path, this.uuid);
 					
				}
			}
		}
		return (String[])candidate_dirs.toArray();
				
	}
	
	//coding
	public void write_inodedirectory_to_dataoutputbuffer(String src, DataOutputBuffer dob) throws IOException {
		//src must be a directory;
		//a single file is not allowed to be migrated unless the rename operation
		INode[] path_nodes = fs_dir.getExistingPathINodes(src);
		INode last_inode = path_nodes[path_nodes.length - 1];
		if (!(last_inode instanceof INodeDirectory)) {
			//sorry the last inode must be inodedirectory
			return ;
		}
		
		int file_counter = 0;
		INodeDirectory target_inode_directory = (INodeDirectory) last_inode;
		for (INode node : target_inode_directory.getChildren()) {
			if (node instanceof INodeFile) {
				file_counter ++;
			}
		}
		
		dob.writeBytes(src + DFSConfigKeys.new_line);
		target_inode_directory.write(dob);
		dob.writeInt(file_counter);
		for (INode node : target_inode_directory.getChildren()) {
			node.write(dob);
		}
		
	} 
	
	//coding
	public void restore_inodedirectory_to_dataoutputbuffer(DataInputBuffer dib) throws IOException {
		
		INodeDirectory inodedirectory = new INodeDirectory("", 
				new PermissionStatus("", "", FsPermission.getDefault()));
		inodedirectory.readFields(dib);
		int inodefile_counter = dib.readInt();
		INodeFile[] inode_files = new INodeFile[inodefile_counter];
		for (int i = 0; i < inode_files.length; i++) {
			INodeFile inodefile = new INodeFile();
			inodefile.readFields(dib);
			inode_files[i] = inodefile;
			inodedirectory.addNode(inodefile.getFullPathName(), inodefile);
		}
				
	}
	
	public static void print_current_line() {
		StringBuilder str_builder = new StringBuilder();
		StackTraceElement ste_obj = new Throwable().getStackTrace()[1];
		str_builder.append(ste_obj.getClassName());
		str_builder.append(".");
		str_builder.append(ste_obj.getMethodName());
		str_builder.append("@");
		str_builder.append(ste_obj.getLineNumber());
		NameNode.LOG.info("[dbg] tracing: " + str_builder.toString());
	}
	
	/*coding
	public boolean dtop_migration_commit(DataInputBuffer dib) throws IOException{
		try {
			
		} catch (Exception e) {
			// TODO: handle exception
		}
		String target = dib.readLine();
		
		
		if (lock_acquired) { // release the lock;
			synchronized (lock_table) {
				for (String path: candidate_dirs) {
					lock_table.remove(path);
				}
			}
		}
		
		return true;
	}*/
	


}
