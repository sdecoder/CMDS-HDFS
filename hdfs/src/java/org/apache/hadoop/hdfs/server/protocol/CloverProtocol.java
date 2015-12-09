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

package org.apache.hadoop.hdfs.server.protocol;

import java.io.IOException;

import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.ipc.VersionedProtocol;

/*****************************************************************************
 * Protocol that cluster Clover NameNode uses to communicate with each other.
 * @param <INodeFile>
 * 
 *****************************************************************************/
public interface CloverProtocol<INodeFile> extends VersionedProtocol {
	public static final long versionID = 1L;

	/**
	 * getUID() shadow RPC as ClientProtocol
	 * @param src
	 * @return
	 * @throws IOException
	 */
	 
 
	/**
	 * Issue a barrier
	 */
	//public boolean barrier(long timestamp, long hint) throws IOException;

	
	
}	
