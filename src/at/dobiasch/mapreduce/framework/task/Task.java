package at.dobiasch.mapreduce.framework.task;

import java.util.ArrayList;
import java.util.List;

public class Task {
	private List<String> worker;
	private int inpid;
	private long partStart;
	private long partEnd;

	public Task(int inpid, long partStart, long partEnd) {
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
}
