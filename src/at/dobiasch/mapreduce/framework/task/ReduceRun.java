package at.dobiasch.mapreduce.framework.task;

import java.util.concurrent.Callable;

import at.dobiasch.mapreduce.framework.FrameworkFactory;
import at.dobiasch.mapreduce.framework.WorkspaceBuffer;

public class ReduceRun implements Callable<Object>
{
	long ID;
	String key;
	IntKeyVal value;
	
	public ReduceRun(long ID, String key, IntKeyVal value)
	{
		this.ID= ID;
		this.key= key;
		this.value= value;
	}

	@Override
	public Object call()
	{
		boolean returned= false;
		WorkspaceBuffer.Element elem= null;
		try
		{
			elem= FrameworkFactory.getInstance().getTaskController().startReduceRun(ID, key, value.fn, value.getFileSize());
			
			
			elem.ws.runCompiledCommands(elem.owner, elem.reduce);
			System.out.println("done compiled command " + ID);
			
			returned= true;
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} finally {
			FrameworkFactory.getInstance().getTaskController().setReduceFinished(ID, returned, elem);
		}
		
		return true;
	}
}
