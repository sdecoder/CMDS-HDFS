package org.apache.hadoop.hdfs;

import java.lang.reflect.Method;
import org.apache.commons.logging.Log;

public class RPCContainerThread extends Thread{
	int selfid = -1;
	Object engine = null;
	Thread mainThread = null;
	Boolean[] calldone = null;
	Object[] callresult = null;
	String methodName = null;
	Class<?>[] parametersType = null;
	Object[] parameters = null;
	Log console = null;

	public void setMainThread(Thread execThread) {
		this.mainThread = execThread;
	}
	
	public void setConsoleLog(Log _log) {
		this.console = _log;
	}

	public void setSelfId(int selfid) {
		this.selfid = selfid;
	}

	public void setCallDoneFlag(Boolean[] calldone) {
		this.calldone = calldone;
	}

	public void setCallResult(Object[] callresult) {
		this.callresult = callresult;
	}

	public void setRPCCoreObj(Object obj) {
		engine = obj;
	}

	public void setTargetMethod(String methodName) {
		this.methodName = methodName;
	}

	public void setTargetParameterType(Class<?>[] parametertypes) {
		this.parametersType = parametertypes;
	}

	public void setTargetParameterValue(Object[] parameters) {
		this.parameters = parameters;
	}
	
	public boolean isTaskDone() {
		synchronized (mainThread) {
			return calldone[selfid];			
		}
	}

	public void run() {
		try {
			//this.console.info("[inf] RPCContainerThread: Starting...");
			Method method = engine.getClass().getMethod(this.methodName, parametersType);
			synchronized (mainThread) {
				calldone[selfid] = false; // just in case
 			}
			callresult[selfid] = method.invoke(engine, this.parameters);
			synchronized (mainThread) {
				calldone[selfid] = true;
				mainThread.notify();
			}
			//this.console.info("[inf] RPCContainerThread: Ended...");
		}catch (Exception e) {
			this.console.error(e.toString());
			for (StackTraceElement ste : e.getStackTrace()) {
				this.console.error(ste);	
			}			
		}

	}

}
