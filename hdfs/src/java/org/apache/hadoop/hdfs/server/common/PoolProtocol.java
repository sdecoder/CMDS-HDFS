package org.apache.hadoop.hdfs.server.common;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.hdfs.server.common.PoolFile.*;




/**********************************************************************
 * PoolProtocol is used by user code via 
 * {@link org.apache.hadoop.hdfs.server.commmon.PoolClient} class 
 * to communicate with the PoolServer.  User code can manipulate 
 * the directory architecture as well as open/close file streams, etc.
 * It also define EditLogFileInputStream and EditLogFileOutputStream 
 * interfaces for rpc operations.
 *
 **********************************************************************/

public interface PoolProtocol extends VersionedProtocol {
	
	public static final long versionID = 90L;
	
	/////////////////////StorageDirectory operation///////////////////
	
	/**
	 * fileReport() tells the NameNode about all the locally-store
	 * files and directories. The NameNode update the sotre information
	 * into logical view dirMap. 
	 * @param files - the file(directory) list as an array of string
	 */
	public int fileReport(String clientName, int rpcPort, int socketPort, PoolFileInfo[] files)
		throws IOException;
	
	/**
	 * rename files or sdirectories 
	 */
	public void rename(PoolFile from, PoolFile to, String clientName) 
				throws IOException;
	
	/**
	 * delete the directory
	 */
	public void fullDelete(PoolFile dir) 
				throws IOException;
	
	/**
	 * create the directory, only modify dirMap in memory
	 */
	public void mkdir(PoolFile dir);
	
	/**
	 * delete the file
	 */
	public boolean delete(PoolFile file);
	
	/**
	 * write /image/fsimage
	 */
	public void writeCorruptedData(PoolFile file) 
				throws IOException;
	
	/**
	 * write version file
	 */
	public void writeVersion(PoolFile file, String[] values, long ckpTime)
				throws IOException;
	
	/**
	 * read version file
	 */
	public Properties readVersion(PoolFile from)
				throws IOException;
	
	/**
	 * check /image/fsimage
	 */
	public boolean isConversionNeeded(PoolFile from)
				throws IOException;

	/**
	 * read ckpttime file
	 */
	public long readCheckpointTime(PoolFile file) 
			throws IOException;

	/**
	 * notify format
	 */
	public void notifyFormat(String name);

	/**
	 * notify report
	 */
	public void notifyReport(String name);

	
	//////////////////EditLogFileInput(Output)Stream interface/////
	
	//EditLogFileInputStream interface
	//parameter edit is associated with editloginputstream in server
	public void newEditLogFileInputStream(PoolFile edit) throws IOException;
	
	public int available(String edit) throws IOException;
	
	public int read(String edit) throws IOException;
	
	public int read(String edit, byte[] b, int off, int len) throws IOException;
	
	public void closeInput(String edit) throws IOException;
	
	public long lengthInput(String edit) throws IOException;
		
	//EditLogFileOutputStream interface
	public void newEditLogFileOutputStream(PoolFile edit, int size, boolean isrole) 
	throws IOException;
	
	public void write(String edit, int b) throws IOException;
	
	public void write(String edit, Writable[] writables) throws IOException;
	
	public void write(String edit, Writable w) throws IOException;

	public void create(String edit) throws IOException;
	
	public void closeOutput(String edit) throws IOException;
	
	public void setReadyToFlush(String edit) throws IOException;
	
	public void flush(String edit) throws IOException;

	//wal trans: uuid status interface
	public String writeTrans(String edit, String uuid, 
		   String status, byte op, Writable[] writables) throws IOException;

	public void createWal(String edit) throws IOException;	

	public void closeWal(String edit) throws IOException;
	
	public String[] loadWalFSEdits(String edit, boolean closeOnExit) throws IOException;

}
