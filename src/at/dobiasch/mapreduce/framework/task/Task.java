package at.dobiasch.mapreduce.framework.task;

import java.util.ArrayList;
import java.util.List;

public class Task {
	private List<String> worker;

	public Task(int inpid, long partStart, long partEnd) {
		// TODO Auto-generated constructor stub
		worker= new ArrayList<String>();
	}

	public void addWorker(String cnode) {
		worker.add(cnode);
	}
}
