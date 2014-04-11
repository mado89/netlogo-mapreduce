package at.dobiasch.mapreduce.framework.multi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import at.dobiasch.mapreduce.framework.task.Task;

public class NodeManager {
	private class NodeItem {
		private String name;
		private List<Task> tasks;
		
		/**
		 * @param name Name of the Node
		 */
		public NodeItem(String name) {
			this.name= name;
			this.tasks= new ArrayList<Task>();
		}
		
		public String getName() {
			return name;
		}
		
		public void addTask(Task t) {
			tasks.add(t);
		}
		
		public void removeTask(Task t) {
			tasks.remove(t);
		}

		public List<Task> getTasks() {
			return tasks;
		}
		
		@Override
		public String toString() {
			return "NodeItem [name=" + name + ", tasks=" + tasks + "]";
		}

	};
	
	Map<String,NodeItem> nodes;
	
	public NodeManager() {
		nodes= new HashMap<String,NodeItem>();
	}

	/**
	 * Create an Iterator over the names of nodes
	 * @return
	 */
	public Iterator<String> iteratorNodeNames() {
		return nodes.keySet().iterator();
	}
	
	/**
	 * 
	 * @return
	 */
	public int size() {
		return nodes.keySet().size();
	}
	
	public void add(String name) {
		nodes.put(name, new NodeItem(name));
	}
	
	/** 
	 * Remove a node from the network
	 * @param name
	 * @return list of tasks which the node should compute
	 */
	public List<Task> removeClient(String name) {
		Object obj= nodes.get(name);
		System.out.println("Node: " + obj + " " + nodes.keySet());
		if( obj != null )
		{
			List<Task> tasks= getNodeTasks(name);
			nodes.remove(name);
			return tasks;
		}
		else
			return new ArrayList<Task>();
	}

	public List<Task> getNodeTasks(String node) {
		return nodes.get(node).getTasks();
	}

	public void addNodeTask(String node, Task t) {
		// System.out.println("addNodeTask" + node + " " + t + " " + nodes.get(node));
		nodes.get(node).addTask(t);
		// System.out.println("addNodeTask2" + node + " " + t + " " + nodes.get(node));
	}

	public void removeNodeTasks(String node) {
		System.out.println("Remove Node Tasks: " + node);
		nodes.get(node).tasks.clear();
	}

}
