package at.dobiasch.mapreduce.framework;

import java.util.HashMap;
import java.util.Map;

import org.nlogo.headless.HeadlessWorkspace;
import org.nlogo.nvm.Workspace;

public class TaskController
{
	public class Data
	{
		public long ID;
		public String src;
		public long start;
		public long end;
		
		public Data(long iD, String src, long start, long end)
		{
			ID = iD;
			this.src = src;
			this.start = start;
			this.end = end;
		}
	}
	// List of Map Tasks
	// List of Keys to be reduced
	Map<Workspace,Data> map;
	
	public TaskController()
	{
		map= new HashMap<Workspace,Data>();
	}
	
	public void addMap(HeadlessWorkspace ws, long ID, String src, long start, long end)
	{
		// System.out.println("Adding Map for " + ws + " " + this);
		Data data= new Data(ID, src, start, end);
		map.put(ws, data);
		// System.out.println("After add " + map);
	}
	
	public void removeMap(HeadlessWorkspace ws)
	{
		map.remove(ws);
	}

	public Data getData(Workspace ws)
	{
		// System.out.println("Getting data for " + ws);
		// System.out.println(" " + this);
		// System.out.println(map);
		return map.get(ws);
	}
}
