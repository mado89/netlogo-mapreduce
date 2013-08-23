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
	
	public TaskManager()
	{
		taskmap= new HashMap<String,Task>();
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
		Task t= new Task(inpid,partStart,partEnd);
		String key= t.getKey();
		
		System.out.println("Create Task: " + inpid + " " + cnode + " " + tfn + " " + partStart);
		
		if( cnode != null )
			mapassignment+= cnode + "-" + tID + "-" + inpid + "-" + partStart + "-" + partEnd + ",";
		else
			controller.addMap(controller.getID(), fn, partStart, partEnd);
		
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

	public void removeNode(String name) {
		// TODO Auto-generated method stub
		/*
		// Go over all tasks and set them to fail
		for(String task : tasks)
			failedTask(task);

	private void failedTask(String task) {
		List<String> nodes= tasknodes.get(task);
		nodes.remove(task);
		
		// TODO: decide about reshedule
		
		if( nodes.size() == 0 )
			tasknodes.remove(task);
	}
		 */
	}

	public void tasksDone(String node) {
		for(Task t : nodes.getNodeTasks(node))
		{
			taskmap.get(t.getKey()).removeWorker(node);
		}
		nodes.removeNodeTasks(node);
		synchronized(this){
			notifyAll();
		}
	}
}
