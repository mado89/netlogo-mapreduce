package at.dobiasch.mapreduce.framework.task;

import java.util.concurrent.Callable;

import at.dobiasch.mapreduce.framework.FrameworkFactory;
import at.dobiasch.mapreduce.framework.WorkspaceBuffer;

public class MapRun implements Callable<Object>
{
	long ID;
	String key;
	long partStart;
	long partEnd;
	
	public MapRun(long ID, String key, long partStart, long partEnd)
	{
		this.ID= ID;
		this.key= key;
		this.partStart= partStart;
		this.partEnd= partEnd;
	}

	@Override
	public Object call()
	{
		boolean returned= false;
		WorkspaceBuffer.Element elem= null;
		try
		{
			elem= FrameworkFactory.getInstance().getTaskController().startMapRun(ID, key, partStart, partEnd);
			
			// System.out.println("run compiled command" + ID);
			elem.ws.runCompiledCommands(elem.owner, elem.map);
			System.out.println("done compiled command" + ID);
			
			returned= true;
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} finally {
			FrameworkFactory.getInstance().getTaskController().setMapFinished(ID, returned, elem);
		}
		
		return true;
	}
}
