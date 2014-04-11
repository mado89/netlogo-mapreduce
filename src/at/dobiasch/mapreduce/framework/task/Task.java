package at.dobiasch.mapreduce.framework.task;

import java.util.ArrayList;
import java.util.List;

public class Task {
	private List<String> worker;
	private int inpid;
	private String fn;
	private long partStart;
	private long partEnd;
	private long tID;

	public Task(int inpid, String fn, long tID, long partStart, long partEnd) {
		this.inpid = inpid;
		this.fn = fn;
		this.tID= tID;
		this.partStart = partStart;
		this.partEnd = partEnd;
		worker= new ArrayList<String>();
	}
	
	public String getKey() {
		return inpid + "-" + partStart + "-" + partEnd;
	}

	public void addWorker(String node) {
		worker.add(node);
	}
	
	public void removeWorker(String node) {
		worker.remove(node);
	}
	
	public List<String> getWorker() {
		return worker;
	}
	
	public int getInpid() {
		return inpid;
	}
	
	public long getTID() {
		return tID;
	}
	
	@Override
	public String toString() {
		return "Task [key:" + getKey() + "]";
	}

	public String getFn() {
		return fn;
	}

	public long getPartStart() {
		return partStart;
	}

	public long getPartEnd() {
		return partEnd;
	}
}
