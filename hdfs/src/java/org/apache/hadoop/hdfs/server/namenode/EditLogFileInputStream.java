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
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.common.PoolFile;
import org.apache.hadoop.hdfs.server.common.PoolFile.NameNodeInfo;
import org.apache.commons.logging.Log;

/**
 * An implementation of the abstract class {@link EditLogInputStream}, which
 * reads edits from a local file.
 */
public class EditLogFileInputStream extends EditLogInputStream {
  private File file;
  private FileInputStream fStream;
  public static final Log LOG = LogFactory.getLog(EditLogFileInputStream.class.getName());
  
  //modify for rpc
  private boolean isLocal;
  private NameNodeInfo target = null; 
  private String edit; //edit file full name, e.g. ${rootpath} + name

  static NameNodeInfo roleNode = null; //wal role node
  
  public EditLogFileInputStream(File name) throws IOException {
    
	/*original code */
	//file = name;
    //fStream = new FileInputStream(name);
	  
    PoolFile f = (PoolFile)name;
    edit = f.getRootpath() +  "/" + f.getName();
    file = new File(edit);
    fStream = new FileInputStream(file);
    isLocal = true;
    if (!file.exists()) {
    	//Need use rpc to read data
    	isLocal = false;
    	List<NameNodeInfo> nodes = f.getNodeInfos();
    	List<NameNodeInfo> filterNodes = new ArrayList<NameNodeInfo>();
    	for (NameNodeInfo node : nodes) {
    		if (NameNode.client.clientName != node.getHostName()) {
    			//remove local node
    			filterNodes.add(node);
    		}
    	}
    	target = filterNodes.get(0);
    	if (target == null || filterNodes.size() == 0) {
    		throw new IOException("No node available for file" + file);
    	}
    	NameNode.client.start(target).newEditLogFileInputStream(f);
    }
    
  }

  //wal edtil log inputstream
  public EditLogFileInputStream(File name, boolean isrole) 
		  throws IOException
  {
	  	PoolFile pf = (PoolFile)name;
	  	edit = pf.getRootpath() + "/" + pf.getName();
	  	file = new File(edit);
	  	
	  	if (pf.getNodeInfos().size() > 0 ) {
			  roleNode = pf.getNodeInfos().get(0);
		} else {
			  throw new IOException("wallogfile nodeinfo not exist");
		}
	  	
	  	NameNode.client.start(roleNode).newEditLogFileInputStream(pf);
	  		  
  }

  @Override // JournalStream
  public String getName() {
    return file.getPath();
  }

  @Override // JournalStream
  public JournalType getType() {
    return JournalType.FILE;
  }

  @Override
  public int available() throws IOException {
	if (isLocal){
		return fStream.available();
    } else {
    	return NameNode.client.start(target).available(edit);
    }
  }

  @Override
  public int read() throws IOException {
	if (isLocal){
		return fStream.read();
	} else {
		return NameNode.client.start(target).read(edit);
	}
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
	if (isLocal) {  
		return fStream.read(b, off, len);
    } else {
    	return NameNode.client.start(target).read(edit, b, off, len);
    }
  }

  @Override
  public void close() throws IOException {
	if (isLocal) { 
		fStream.close();
	} else {
		NameNode.client.start(target).closeInput(edit);
	}
  }

  @Override
  public long length() throws IOException {
    // file size + size of both buffers
	if (isLocal) {
		return file.length();
	} else {
		return NameNode.client.start(target).lengthInput(edit);
	}
  }
}
