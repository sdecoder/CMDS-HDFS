package org.apache.hadoop.hdfs.server.common;

public interface HdfsBlockConst {
	  /**
	   * Block id regions: <16 bits node id><48 bits file id>
	   */
	  static final long BLOCKID_NODE_MASK = 0xffff000000000000L;
	  static final int  BLOCKID_NODE_SHIFT = 48;
}
