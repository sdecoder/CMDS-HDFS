/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.PoolFile;
import org.apache.hadoop.hdfs.server.common.StorageClover;
import org.apache.hadoop.hdfs.server.common.PoolFile.NameNodeInfo;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.hdfs.DeprecatedUTF8;

/**
 * An implementation of the abstract class {@link EditLogOutputStream}, which
 * stores edits in a local file.
 */
public class EditLogFileOutputStream extends EditLogOutputStream {
  private static int EDITS_FILE_HEADER_SIZE_BYTES = Integer.SIZE / Byte.SIZE;
  public static final Log LOG = LogFactory.getLog(EditLogFileOutputStream.class.getName());
  
  private File file;
  private FileOutputStream fp; // file stream for storing edit logs
  private FileChannel fc; // channel of the file stream for sync
  private DataOutputBuffer bufCurrent; // current buffer for writing
  private DataOutputBuffer bufReady; // buffer ready for flushing
  static ByteBuffer fill = ByteBuffer.allocateDirect(512); // preallocation

  //modify for rpc
  private List<NameNodeInfo> filterNodes = new ArrayList<NameNodeInfo>(); 
  private String edit; //edit file full name, e.g. ${rootpath} + name
  private boolean needRpc = true; //whether need rpc
 
  static NameNodeInfo roleNode = null; //wal role node

  /**
   * Creates output buffers and file object.
   * 
   * @param name
   *          File name to store edit log
   * @param size
   *          Size of flush buffer
   * @throws IOException
   */
  public EditLogFileOutputStream(File name, int size) throws IOException {
    super();
   
    //LOG.info("enter editlogfile outputstream");
    PoolFile f = null;
    if (name instanceof PoolFile) {
    	f = (PoolFile) name;
    	edit = f.getRootpath() + "/" + f.getName();
    	file = new File(edit);
	needRpc = true;
    	//LOG.info("log is poolfile: " + edit + ", nodeinfos: " + f.getNodeInfos().size() + ", needRpc: " + needRpc);
    } else {
    	edit = name.getPath();
    	file = new File(edit);
	needRpc = false;
    	//LOG.info("log is file: " + edit + ", needrpc: " + needRpc);
    			
    }
            
    //original code
    //file = name;
    bufCurrent = new DataOutputBuffer(size);
    bufReady = new DataOutputBuffer(size);
    //RandomAccessFile rp = new RandomAccessFile(f, "rw");
    RandomAccessFile rp = new RandomAccessFile(file, "rw");
    fp = new FileOutputStream(rp.getFD()); // open for append
    fc = rp.getChannel();
    fc.position(fc.size());
    //LOG.info("get fp for log :" + file.getPath());      

    if (needRpc) {	
    	for (NameNodeInfo node : f.getNodeInfos()) {
    		if (!NameNode.client.clientName.equalsIgnoreCase(node.getHostName())) {
    			//remove local node
    			filterNodes.add(node);
    		}
    	}


    	if (filterNodes.size() == 0) {
    		//LOG.info("No remote node available for file" + file);
    	}
    	//call RPC to multiple nodes
    	for (NameNodeInfo node : filterNodes) {
    		NameNode.client.start(node).newEditLogFileOutputStream(f, size, false);
    	}    	
    }
    
  }

  //for create wallogfile outputstream
  public EditLogFileOutputStream(File name, int size, boolean isrole) 
		  throws IOException {
	  super();
	 
	  PoolFile pf = (PoolFile) name;
	  edit = pf.getRootpath() + "/" + pf.getName();
	  file = new File(edit);
	  
	  if (pf.getNodeInfos().size() > 0 ) {
		  roleNode = pf.getNodeInfos().get(0);
	  } else {
		  throw new IOException("wallogfile nodeinfo not exist");
	  }
	  
	  NameNode.client.start(roleNode).newEditLogFileOutputStream(pf, size, true);
	  
  }

  @Override // JournalStream
  public String getName() {
    return file.getPath();
  }

  @Override // JournalStream
  public JournalType getType() {
    return JournalType.FILE;
  }

  /** {@inheritDoc} */
  @Override
  public void write(int b) throws IOException {
	synchronized (this) {
	  bufCurrent.write(b);
	}
	  
	//call RPC to write remote nodes
	if (needRpc) {
	  for (NameNodeInfo node : filterNodes) {
		  NameNode.client.start(node).write(edit, b);
	  }  
	}  
  }

  /** {@inheritDoc} */
  @Override
  public void write(byte op, Writable... writables) throws IOException {
	synchronized (this) {
		////LOG.info("enter write: op, writable...");
		bufCurrent.write(op);
		for (Writable w : writables) {
			w.write(bufCurrent);
		}
	}

	//call RPC to remote nodes
	if (needRpc) {
		for (NameNodeInfo node : filterNodes) {
			NameNode.client.start(node).write(edit, op);
			for (Writable w : writables) {
				if (w instanceof ArrayWritable) {
				  Writable[] arr = ((ArrayWritable) w).get();
				  NameNode.client.start(node).write(edit, arr);
				} else {
				  NameNode.client.start(node).write(edit, w);
				}
			}
		}
	}

  }

  //only for rpc call
  public void write(Writable[] writables) throws IOException {
	synchronized (this) {
		////LOG.info("enter write: writable[]: " + writables.length);
		//bufCurrent.writeInt(writables.length);
		bufCurrent.writeInt(writables.length);
	}
	for (Writable w : writables) {
		w.write(bufCurrent);
	}
  }

  //only for rpc call
  public void write(Writable w) throws IOException {
	synchronized (this) {
	//	//LOG.info("enter write: writable");
		w.write(bufCurrent);
	}
  }


  /**
   * Create empty edits logs file.
   */
  @Override
  public synchronized void create() throws IOException {
	//LOG.info("enter create edit:" + edit);
	fc.truncate(0);
	fc.position(0);
		
	bufCurrent.writeInt(FSConstants.LAYOUT_VERSION);
	/*setReadyToFlush()*/
	assert bufReady.size() == 0 : "previous data is not flushed yet";
	bufCurrent.write(FSEditLog.OP_INVALID); //insert end-of-file marker
	DataOutputBuffer tmp = bufReady;
	bufReady = bufCurrent;
	bufCurrent = tmp;
	/*flush()*/
	numSync++;
	long start = FSNamesystem.now();
	flushAndSync();
	long end = FSNamesystem.now();
	totalTimeSync += (end - start);
		

	//call RPC to remote nodes
	if (needRpc) { 
	  for (NameNodeInfo node : filterNodes) {
		  NameNode.client.start(node).create(edit);
	  }  
	} 
  }

  @Override
  public synchronized void close() throws IOException {
    // close should have been called after all pending transactions
    // have been flushed & synced.
    
    //LOG.info("enter close edit: " + edit); 
    
    int bufSize = bufCurrent.size();
    if (bufSize != 0) {
      throw new IOException("FSEditStream has " + bufSize
          + " bytes still to be flushed and cannot " + "be closed.");
    }
    bufCurrent.close();
    bufReady.close();

    // remove the last INVALID marker from transaction log.
    fc.truncate(fc.position());
    fp.close();

    bufCurrent = bufReady = null;
   
    //call RPC to remote nodes
    if (needRpc) {
    	for (NameNodeInfo node : filterNodes) {
		NameNode.client.start(node).closeOutput(edit);
	} 
    }
  }

  /**
   * All data that has been written to the stream so far will be flushed. New
   * data can be still written to the stream while flushing is performed.
   */
  @Override
  public synchronized void setReadyToFlush() throws IOException {
    //LOG.info("enter set ready to flush");
    assert bufReady.size() == 0 : "previous data is not flushed yet";
    bufCurrent.write(FSEditLog.OP_INVALID); // insert end-of-file marker
    DataOutputBuffer tmp = bufReady;
    bufReady = bufCurrent;
    bufCurrent = tmp;
    
	//call RPC to remote nodes
	if (needRpc) {
	  for (NameNodeInfo node : filterNodes) {
		NameNode.client.start(node).setReadyToFlush(edit);
	  } 
	}
  }
  
   @Override
   public synchronized void flush() throws IOException {
    	//LOG.info("enter flush: " + edit);
	numSync++;
	long start = FSNamesystem.now();
	flushAndSync();
	long end = FSNamesystem.now();
	totalTimeSync += (end - start);
	
	//call RPC to remote nodes
	if (needRpc) {
		for (NameNodeInfo node : filterNodes) {
			NameNode.client.start(node).flush(edit);
		}
	}

   }
	

  /**
   * Flush ready buffer to persistent store. currentBuffer is not flushed as it
   * accumulates new log records while readyBuffer will be flushed and synced.
   */
  @Override
  protected void flushAndSync() throws IOException {
    preallocate(); // preallocate file if necessary
    bufReady.writeTo(fp); // write data to file
    bufReady.reset(); // erase all data in the buffer
    fc.force(false); // metadata updates not needed because of preallocation
    fc.position(fc.position() - 1); // skip back the end-of-file marker
  }

  /**
   * Return the size of the current edit log including buffered data.
   */
  @Override
  public long length() throws IOException {
    // file size - header size + size of both buffers
    return fc.size() - EDITS_FILE_HEADER_SIZE_BYTES + bufReady.size()
        + bufCurrent.size();
  }

  // allocate a big chunk of data
  private void preallocate() throws IOException {
    long position = fc.position();
    if (position + 4096 >= fc.size()) {
      FSNamesystem.LOG.debug("Preallocating Edit log, current size "
          + fc.size());
      long newsize = position + 1024 * 1024; // 1MB
      fill.position(0);
      int written = fc.write(fill, newsize);
      FSNamesystem.LOG.debug("Edit log size is now " + fc.size() + " written "
          + written + " bytes " + " at offset " + newsize);
    }
  }

  /**
   * Operations like OP_JSPOOL_START and OP_CHECKPOINT_TIME should not be
   * written into edits file.
   */
  @Override
  public boolean isOperationSupported(byte op) {
    return op < FSEditLog.OP_JSPOOL_START - 1;
  }

  /**
   * Returns the file associated with this stream.
   */
  File getFile() {
    return file;
  }
}
