package at.dobiasch.mapreduce.framework.multi;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class NodeManager {
	
	List<String> nodes;
	
	public NodeManager() {
		nodes= new ArrayList<String>();
	}

	/**
	 * Create an Iterator over the names of nodes
	 * @return
	 */
	public Iterator<String> iteratorNodeNames() {
		return nodes.iterator();
	}
	
	/**
	 * 
	 * @return
	 */
	public int size() {
		return nodes.size();
	}
	
	public void add(String name) {
		nodes.add(name);
	}

	public void removeClient(String name) {
		nodes.remove(name);
	}

}
