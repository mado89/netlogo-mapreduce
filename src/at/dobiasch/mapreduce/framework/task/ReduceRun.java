package at.dobiasch.mapreduce.framework.task;

import java.util.concurrent.Callable;

import org.nlogo.api.ExtensionException;

import at.dobiasch.mapreduce.framework.FrameworkFactory;
import at.dobiasch.mapreduce.framework.WorkspaceBuffer;

public class ReduceRun implements Callable<Object>
{
	long ID;
	String key;
	IntKeyVal value;
	int partition;
	
	public ReduceRun(long ID, String key, IntKeyVal value, int partition)
	{
		this.ID= ID;
		this.key= key;
		this.value= value;
		this.partition= partition;
	}

	@Override
	public Object call()
	{
		boolean returned= false;
		WorkspaceBuffer.Element elem= null;
		boolean excep= false;
		try
		{
			elem= FrameworkFactory.getInstance().getTaskController().startReduceRun(ID, key, value.fn, value.getFileSize(),partition);
			
			elem.ws.runCompiledCommands(elem.owner, elem.read);
			elem.ws.runCompiledCommands(elem.owner, elem.reduce);
			// elem.ws.runCompiledCommands(elem.owner, elem.clean);
			
			excep= elem.ws.lastLogoException() != null;
			System.out.println("done compiled command " + ID + ": " + excep);
			
			returned= true;
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} finally {
			boolean suc = returned && !excep;
			// System.out.println(this.ID + " returned" + returned + " excep" + excep);
			if( excep )
				elem.ws.clearLastLogoException();
			try {
				FrameworkFactory.getInstance().getTaskController().setReduceFinished(ID, suc, elem, key, value);
			} catch (ExtensionException e) {
				e.printStackTrace();
				return false;
			}
		}
		
		return true;
	}
}
