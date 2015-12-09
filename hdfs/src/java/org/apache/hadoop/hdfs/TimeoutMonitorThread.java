package org.apache.hadoop.hdfs;

public class TimeoutMonitorThread extends Thread {

	private long timeout;
	private boolean isCanceled = false;
	private TimeoutMonitorException timeoutMonitorException;
	private Thread inspectedTarget;

	public TimeoutMonitorThread(long timeout, Thread inspectedTarget, TimeoutMonitorException timeoutErr) {
		super();
		this.timeout = timeout;
		this.timeoutMonitorException = timeoutErr;
		this.inspectedTarget = inspectedTarget;
		this.setDaemon(true);
	}

	public synchronized void stopTiming() {
		synchronized (this) {
			isCanceled = true;
			this.notify();
		}
	}

	public void run() {
		try {
			
			synchronized (this) {
				this.wait(timeout);
				if (!isCanceled)
					throw this.timeoutMonitorException;
			}

		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (TimeoutMonitorException e) {
			//inspected target thread timeout here!
			//interrupt it;
			inspectedTarget.interrupt();
		}
	}
}
