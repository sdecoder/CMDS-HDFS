package org.apache.hadoop.hdfs.server.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class PoolFileInfo implements Writable{
	
	//name is path without ${root}, like node1/current/fsimage,
	// node1/checkpointXXX/node1/current/fsimage
	public String name = "";
	protected boolean isDir = false;
	//for fsimage and edit file, need crc
	public int crc = 0;
	
	PoolFileInfo() {
		this.name = "";
		this.isDir = false;
		this.crc = 0;
	}

	PoolFileInfo(String name, boolean isDir, int crc) {
		this.name = name;
		this.isDir = isDir;
		this.crc = crc;
	}
	
	PoolFileInfo(String name, int crc) {
		this.name = name;
		this.crc = crc;			
	}
	
	PoolFileInfo(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
	
	public int getCrc() {
		return crc;
	}
	
	//implement write of Writable
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, name);
		out.writeInt(crc);			
	}
	
	//implement readfields of Writable
	public void readFields(DataInput in) throws IOException {
		name = Text.readString(in);
		in.readInt();
	}
			
	//return node dir name, like node1
	public String getNodeDirName() {
		int slash = 0;
		String tmpStr = "";
		slash = name.indexOf('/');
		tmpStr = name.substring(0, slash);
		return tmpStr; 
		
	}
	
	//return dir name, like current or checkpointXXX
	public String getDirName() {
		int slash = 0;
		String tmpStr = "";
		slash = name.indexOf('/');
		tmpStr = name.substring(slash+1);
		slash = tmpStr.indexOf('/');
		tmpStr = tmpStr.substring(0, slash);
		return tmpStr;
	}
	
	//return file name like fsimage,edit or node1/current/fsimage
	public String getFileName() {
		int slash = 0;
		String tmpStr = "";
		slash = name.indexOf('/');
		tmpStr = name.substring(slash+1);
		slash = tmpStr.indexOf('/');
		tmpStr = tmpStr.substring(slash+1);
		return tmpStr;
	}
	
	//return file path name without ${root}, like node1/current/fsimage
	public String getFilePathName() {
		StringBuilder buf = new StringBuilder();
		buf.append(getNodeDirName() + "/" + getDirName() +
				"/" + getFileName());
		return buf.toString();
	}
	
}
