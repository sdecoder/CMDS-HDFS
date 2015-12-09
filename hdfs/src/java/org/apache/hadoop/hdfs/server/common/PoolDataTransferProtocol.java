package org.apache.hadoop.hdfs.server.common;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.hdfs.server.common.PoolFile.NameNodeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Transfer data to/from pool storage using a streaming protocol.
 */

public interface PoolDataTransferProtocol {
	
	/** Version for data transfers between clients and datanodes
	 * This should change when serialization of DatanodeInfo, not just
	 * when protocol changes. It is not very obvious. 
	 */
	/*
	 * Version 19:
	 *    Change the block packet ack protocol to include seqno,
	 *    numberOfReplies, reply0, reply1, ...
	 */
	public static final int POOLDATA_TRANSFER_VERSION = 18;
	public static final Log LOG = LogFactory.getLog(PoolDataTransferProtocol.class.getName());
	
	/** Operation */
	public enum Op {
		WRITE_FILE((byte)80),
		READ_FILE((byte)81),
		READ_METAFILE((byte)82),
		APPEND_FILE((byte)83),
		FILE_CHECKSUM((byte)84);
	
		/** The code for this operation. */
		public final byte code;
	
		private Op(byte code) {
			this.code = code;
		}
		private static final int FIRST_CODE = values()[0].code;
		/** Return the object represented by the code */
		private static Op valueOf(byte code) {
			final int i = (code & 0xff) - FIRST_CODE;
			return i< 0 || i>= values().length ? null : values()[i];
		}
		/** Read from in */
		public static Op read(DataInput in) throws IOException {
			return valueOf(in.readByte());
		}
		/** Write to out */
		public void write(DataOutput out) throws IOException {
			out.write(code);
		}		
	}
	
	/** status **/
	public enum Status {
		SUCCESS(0),
		ERROR(1),
		ERROR_CHECKSUM(2),
		ERROR_INVALID(3),
		ERROR_EXISTS(4),
		ERROR_ACCESS_TOKEN(5),
		CHECKSUM_OK(6);
		
		/** the code for this operation. */
		private final int code;
		
		private Status(int code) {
			this.code = code;
		}
		
		private static final int FIRST_CODE = values()[0].code;
		/** Return the object represented by the code. */
		private static Status valueOf(int code) {
			final int i = code - FIRST_CODE;
			return i < 0 || i >= values().length ? null : values()[i];
		} 
		
		/** read from in */
		public static Status read(DataInput in) throws IOException {
			return valueOf(in.readShort());
		}
		
		/** write to out */
		public void write(DataOutput out) throws IOException {
			out.writeShort(code);
		}
		
	    /** Write to out */
	    public void writeOutputStream(OutputStream out) throws IOException {
	      out.write(new byte[] {(byte)(code >>> 8), (byte)code});
	    }
	};
		
	public enum FileConstructionStage {
		/** The enumerates are always listed as regular stage followed by the
	     * recovery stage. 
	     * Changing this order will make getRecoveryStage not working.
	     */
		// pipeline setup for file append
		PIPELINE_SETUP_APPEND,
		// pipeline setup for failed PIPELINE_SETUP_APPEND recovery
		PIPELINE_SETUP_APPEND_RECOVERY,
		// data streaming
		DATA_STREAMING,
		// pipeline setup for failed data streaming recovery
		PIPELINE_SETUP_STREAMING_RECOVERY,
		//close the file and pipeline
		PIPELINE_CLOSE,
		// recover a failed PIPELINE_CLOSE
		PIPELINE_CLOSE_RECOVERY,
		// pipeline setup for file creation
		PIPELINE_SETUP_CREATE;
		
		final static private byte RECOVERY_BIT = (byte)1;
		
		/**
		 * get the recovery stage of this stage
		 */
		public FileConstructionStage getRecoveryStage() {
			if (this == PIPELINE_SETUP_CREATE) {
				throw new IllegalArgumentException ("Unexcepted fileStage " + this);
			} else {
				return values()[ordinal() | RECOVERY_BIT];
			}
		}
		
		private static FileConstructionStage valueOf(byte code) {
			return code < 0 || code >= values().length ? null : values()[code];
		}
		
		// read from in
		private static FileConstructionStage readFields(DataInput in)
			throws IOException {
			return valueOf(in.readByte());
		}
		
		// write to out
		private void write(DataOutput out) throws IOException {
			out.writeByte(ordinal());
		}
		
	};
		
	
	/** Sender */
	public static class Sender {
		/** Initialize a operation. */
		public static void op(DataOutputStream out, Op op) throws IOException {
			LOG.info("OPwrite version:" + PoolDataTransferProtocol.POOLDATA_TRANSFER_VERSION + ", op:"
				 + Op.WRITE_FILE);
			out.writeShort(PoolDataTransferProtocol.POOLDATA_TRANSFER_VERSION);
			op.write(out);
		}
		
		/** Send OP_WRITE_FILE */
		public static void opWriteFile(DataOutputStream out, String rootpath, 
					String filename, int pipelineSize, FileConstructionStage stage,
					long newGs, String client, NameNodeInfo[] targets) 
					throws IOException {
			op(out, Op.WRITE_FILE);
			
			Text.writeString(out, rootpath);
			Text.writeString(out, filename);
			out.writeInt(pipelineSize);
			stage.write(out);
			WritableUtils.writeVLong(out, newGs);
			Text.writeString(out, client);
			out.writeInt(targets.length - 1);
			LOG.info("OPwrite rootpath:" + rootpath + ", name:" + filename + ", pipeline size:" 
				+ pipelineSize + ", client:" + client + ", targets len:" + (targets.length-1));
			for (int i = 1; i< targets.length; i++) {
				targets[i].write(out);
				LOG.info("Opwrite target[" + i + "]:" + targets[i].getHostPortName());
			}			
		}
		
		/** Send OP_READ_FILE */
		public static void opReadFile(DataOutputStream out,
				String rootpath, String filename, long offset, 
				String clientName) throws IOException {
			op(out, Op.READ_FILE);
			
			Text.writeString(out, rootpath);
			Text.writeString(out, filename);
			out.writeLong(offset);
			Text.writeString(out, clientName);
			out.flush();
		}
	}
		
	/** Receiver */
	public static abstract class Receiver {
		/** Read an Op. It also checks protocol version */
		protected final Op readOp(DataInputStream in) throws IOException {
			final short version = in.readShort();
			LOG.info("read version:" + version + ", and Op");
			if (version != POOLDATA_TRANSFER_VERSION) {
				throw new IOException("Version Mismatch (Excepted: " +
						PoolDataTransferProtocol.POOLDATA_TRANSFER_VERSION +
						", Received: " + version + ")");
			}
			return Op.read(in);
		}
		
		/** Processs op by the corresponding method. */
		protected final void processOp(Op op, DataInputStream in) 
			throws IOException {
			switch(op) {
				case READ_FILE:
					opReadFile(in);
					break;
				case WRITE_FILE:
					opWriteFile(in);
					break;
				default:
					throw new IOException("Unkown op" + op + "in data stream");			
			}			
		} 
		
		/** Receive OP_READ_FILE */
		private void opReadFile(DataInputStream in) throws IOException {
			final String rootpath = Text.readString(in);
			final String filename = Text.readString(in);
			final long startOffset = in.readLong();
			final String client = Text.readString(in);
			
			opReadFile(in, rootpath, filename, startOffset, client);	
		}
		
		/**
		 * Abstract OP_READ_FILE method
		 * Read a file.
		 */
		protected abstract void opReadFile(DataInputStream in, 
				String rootpath, String fileanme, long startOffset,
				String client) throws IOException;
				
		/** Receive OP_WRITE_FILE **/
		private void opWriteFile(DataInputStream in) throws IOException {
			final String rootpath = Text.readString(in); //root path
			final String filename = Text.readString(in); //file name
			final int pipelineSize = in.readInt(); //num of namenodes in entire pipeline
			final FileConstructionStage stage = FileConstructionStage.readFields(in);
			final long newGs = WritableUtils.readVLong(in);
			final String client = Text.readString(in); //working on behalf o this client
			
			final int nTargets = in.readInt();
			if (nTargets < 0) {
				throw new IOException("Mislabelled incoming datastream.");
			}
			LOG.info("receive rootpath:" + rootpath + ", filename:" + filename + ", pipelinesize:"
					+ pipelineSize + ", client:" + client + ", targets len:" + nTargets);
			final NameNodeInfo targets[] = new NameNodeInfo[nTargets];
			for (int i=0; i < targets.length; i++) {
				targets[i] = PoolFile.read(in);
				LOG.info("get targets[" + i + "]:" + targets[i].getHostPortName());
			}
			opWriteFile(in, rootpath, filename, pipelineSize, stage,
					newGs, client, targets);			
		}
		
		/**
		 * Abstract OP_WRITE_FILE method
		 * write a file
		 */
		protected abstract void opWriteFile(DataInputStream in, String rootpath, 
				String filename, int pipelineSize, FileConstructionStage stage,
				long newGs, String client, NameNodeInfo[] targets) throws IOException;			
	}
	
	/** reply **/
	public static class PipelineAck implements Writable {
		private long seqno;
		private Status replies[];
		public final static long UNKOWN_SEQNO = -2;
		
		/** default constructor **/
		public PipelineAck() {}
		
		/**
		 * Constructor
		 * @param seqno sequence number
		 * @param replies an array of replies
		 */
		public PipelineAck(long seqno, Status[] replies) {
			this.seqno = seqno;
			this.replies = replies;
		}
		
		/**
		 * Get the sequence number
		 * @return the sequence number
		 */
		public long getSeqno() {
			return seqno;
		}  		
		
		/**
		 * Get the number of replies
		 * @return the number of replies
		 */
		public short getNumOfReplies() {
			return (short)replies.length;
		}
		
		/**
		 * get the ith reply
		 * @return the ith reply
		 */
		public Status getReply(int i) {
			if (i<0 || i >= replies.length) {
				throw new IllegalArgumentException("The input parameter " +
						i + "shoulde in the range of [0, " + replies.length);
			}
			return replies[i];	
		}
		
		/**
		 * Check if this ack contains error status
		 * @return true if all statuses are SUCCESS
		 */
		public boolean isSuccess() {
			for (Status reply : replies) {
				if (reply != Status.SUCCESS) {
					return false;
				}
			}
			return true;
		}
		
		/**** Writable interface ****/
		@Override //Writable
		public void readFields(DataInput in) throws IOException {
			seqno = in.readLong();
			short numOfReplies = in.readShort();
			replies = new Status[numOfReplies];
			LOG.info("get pipeline ack, seqno:" + seqno + 
				", numofreplies:" + numOfReplies);
			for (int i=0; i<numOfReplies; i++) {
				replies[i] = Status.read(in);
				LOG.info("get pipeline ack, replies[" + i 
					+ "]:" + replies[i]);
			}
		}
		
		@Override //Writable
		public void write(DataOutput out) throws IOException {
			out.writeLong(seqno);
			out.writeShort((short)replies.length);
			for (Status reply : replies) {
				reply.write(out);
			}
		}
		
		public String toString() {
			StringBuilder ack = new StringBuilder("Replies for seqno ");
			ack.append(seqno).append("are");
			for(Status reply : replies) {
				ack.append(" ");
				ack.append(reply);
			}
			return ack.toString();
		}		
	}
			
}
