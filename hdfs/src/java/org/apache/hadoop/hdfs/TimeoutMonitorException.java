package org.apache.hadoop.hdfs;

public class TimeoutMonitorException extends RuntimeException  {
	private static final long serialVersionUID = -8078853655388692688L;
	public TimeoutMonitorException(String errMessage){
		super(errMessage);
	}	

}
