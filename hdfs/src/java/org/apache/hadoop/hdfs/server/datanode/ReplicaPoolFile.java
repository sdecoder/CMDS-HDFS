package org.apache.hadoop.hdfs.server.datanode;

public class ReplicaPoolFile {
	 private String rootpath;
	 private String filename;
	 private long numBytes;
	 
	 public ReplicaPoolFile() {this("", "");}
	 
	 public ReplicaPoolFile(String rootpath, String filename) {
		  set(rootpath, filename, 0);
	 }

	 public ReplicaPoolFile(String rootpath, String filename, long len) {
		  set(rootpath, filename, len);
	 }
	 
	 public ReplicaPoolFile(ReplicaPoolFile rpf) {
		 this(rpf.rootpath, rpf.filename, rpf.numBytes);
	 }
	 
	 public void set(String rootpath, String filename, long len) {
		 this.rootpath = rootpath;
		 this.filename = filename;
		 this.numBytes = len;
	 }
	 
	 public String getRootpath() {
		 return rootpath;
	 }
	 
	 public void setRootpath(String rootpath) {
		 this.rootpath = rootpath;
	 }
	 
	 public String getFilename() {
		 return filename;
	 }
	 
	 public void setFilename(String filename) {
		 this.filename = filename;
	 }
	 
	 //return only name, not include prefix path
	 public String getName() {
		 return filename.substring(filename.lastIndexOf('/')+1);
	 }

	 public long getNumBytes() {
	 	 return numBytes;
	 }
	 public void setNumBytes(long len) {
	 	 this.numBytes = len;
	 }

	 public String toString() {
	    return getFilename() ;
	 }
	 
	  /////////////////////////////////////
	  // Writable???????
	  /////////////////////////////////////
	 
	
}