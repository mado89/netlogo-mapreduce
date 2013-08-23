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
	
	public void removeClient(String name) {
		nodes.remove(name);
	}

	public List<Task> getNodeTasks(String node) {
		return nodes.get(node).getTasks();
	}

	public void addNodeTask(String cnode, Task t) {
		nodes.get(cnode).addTask(t);
	}

	public void removeNodeTasks(String node) {
		nodes.get(node).tasks.clear();
	}

}
