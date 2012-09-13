package at.dobiasch.mapreduce.framework.task;

import java.util.concurrent.Callable;

import at.dobiasch.mapreduce.framework.FrameworkFactory;
import at.dobiasch.mapreduce.framework.WorkspaceBuffer;

public class ReduceRun implements Callable<Object>
{
	long ID;
	String key;
	IntKeyVal value;
	WorkspaceBuffer wb;
	
	public ReduceRun(long ID, String key, IntKeyVal value, WorkspaceBuffer wb)
	{
		this.ID= ID;
		this.key= key;
		this.value= value;
		this.wb= wb;
	}

	@Override
	public Object call()
	{
		boolean returned= false;
		WorkspaceBuffer.Element elem= null;
		try
		{
			elem= wb.get();
			FrameworkFactory.getInstance().getTaskController().addReduce(elem.ws, ID, key, value.fn, value.getFileSize());
			
			elem.ws.runCompiledCommands(elem.owner, elem.reduce);
			System.out.println("done compiled command" + ID);
			
			FrameworkFactory.getInstance().getTaskController().removeReduce(elem.ws);
			
			wb.release(elem);
			
			returned= true;
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} finally {
			if( !returned && elem != null )
				wb.release(elem);
		}
		
		return true;
	}
}
