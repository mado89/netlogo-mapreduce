package at.dobiasch.mapreduce.framework.task;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import at.dobiasch.mapreduce.framework.controller.HostController;
import at.dobiasch.mapreduce.framework.multi.NodeManager;

public class TaskManager {

	private NodeManager nodes;
	private int tpn; // Tasks per node
	private int tfn; // Tasks for current node
	private String cnode; //Current node
	private Iterator<String> nit;
	private Map<String,Task> taskmap;
	private String mapassignment; //Initial Map-Assignment (to send it out)
	private HostController controller;
	private int localtasks;
	private int nodetasks;
	private int finishednodetasks;
	
	public TaskManager()
	{
		taskmap= new HashMap<String,Task>();
		localtasks= 0;
		nodetasks= 0;
		finishednodetasks= 0;
	}

	public void setNodes(NodeManager nodes) {
		this.nodes= nodes;
	}

	public void initMap(int npartitions, HostController controller) {
        tpn= npartitions / ( nodes.size() + 1 );
        tfn= 0;
        nit= nodes.iteratorNodeNames();
        if( nodes.size() > 0 )
        {
        	cnode= nit.next();
        }
        mapassignment= "MAP:";
        this.controller= controller;
	}

	public void assignMapTask(long tID, long partStart, long partEnd, int inpid, String fn) {
		Task t= new Task(inpid,fn,tID,partStart,partEnd);
		String key= t.getKey();
		
		System.out.println("Create Task: " + inpid + " " + cnode + " " + tfn + " " + partStart);
		
		if( cnode != null ) {
			mapassignment+= cnode + "-" + tID + "-" + inpid + "-" + partStart + "-" + partEnd + ",";
			nodetasks++;
		}
		else {
			// controller.addMap(controller.getID(), fn, partStart, partEnd);
			controller.addMap(tID, fn, partStart, partEnd);
			localtasks++;
		}
		
		if( !taskmap.containsKey(key) )
		{
			taskmap.put(key,t);
		}
		else
			t= taskmap.get(key);
		
		t.addWorker(cnode);
		if( cnode != null )
			nodes.addNodeTask(cnode,t);
        
        // Check if the maximum of tasks for this node is added
        tfn++;
        if( tfn == tpn )
        {
                if( nit.hasNext() )
                	cnode= nit.next();
                else
                	cnode= null;
                tfn= 0;
        }
	}
	
	public void taskDone(String node, long ID) {
		
	}
	
	/**
	 * Check whether all map tasks assigned to other nodes are done
	 * @return
	 */
	public boolean allMapsDone() {
		Iterator<String> nit= nodes.iteratorNodeNames();
		
		while( nit.hasNext() ) {
			String node= nit.next();
			if( nodes.getNodeTasks(node).size() > 0 )
				return false;
		}
		
		return true;
	}
	
	public String getMapAssignmentString() {
		return mapassignment;
	}

	/*
	 * A node has failed.
	 * This means all it tasks need to be removed and resheduled
	 */
	public void removeNode(String name) {
		List<Task> tasks= nodes.removeClient(name);
		
		// Go over all tasks and set them to fail
		for(Task task : tasks)
			failedTask(name, task);
		
		synchronized (this) {
			notifyAll();
		}
	}

	/**
	 * A task has failed on a node
	 * Reshedule it
	 * @param task
	 */
	private void failedTask(String node, Task task) {
		// Old code. Was in comment, never used
		/*List<String> nodes= tasknodes.get(task);
		nodes.remove(task);
		
		// TOD-0: decide about reshedule
		
		if( nodes.size() == 0 )
			tasknodes.remove(task);
		*/
		
		// taskmap.get(task.getKey());
		task.removeWorker(node);
		if( task.getWorker().size() == 0 ) // TODO: better alg for reschedule
		{
			controller.addMap(task.getTID(), task.getFn(), task.getPartStart(), task.getPartEnd());
			System.out.println("Task " + task.getTID() + " rescheduled to localhost");
			localtasks++;
			nodetasks--;
		}
	}

	/**
	 * Remove the node as worker from the tasks assigned to the node
	 * Remove the assigned tasks from the node
	 * @param node
	 */
	public void tasksDone(String node) {
		System.out.println("All tasks done for node: " + node);
		for(Task t : nodes.getNodeTasks(node))
		{
			taskmap.get(t.getKey()).removeWorker(node);
			finishednodetasks++;
		}
		nodes.removeNodeTasks(node);
		synchronized(this){
			notifyAll();
		}
	}
	
	public double getMapProgress(double localMapProgress) {
		return (localtasks * localMapProgress + finishednodetasks) / (double) (localtasks + nodetasks);
	}

	public void debugNodes() {
		System.out.println("DEBUG NODES - START");
		if( nodes != null ) {
			Iterator<String> nodeit= nodes.iteratorNodeNames();
			while(nodeit.hasNext())
			{
				String node= nodeit.next();
				System.out.println("Node: " + node + " " + nodes.getNodeTasks(node));
			}
		}else{
			System.out.println("No nodemanager installed");
		}
		System.out.println("DEBUG NODES - END");
	}
}
