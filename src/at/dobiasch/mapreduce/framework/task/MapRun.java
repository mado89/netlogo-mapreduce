package at.dobiasch.mapreduce.framework.task;

import java.util.concurrent.Callable;

import org.nlogo.api.ExtensionException;

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
		boolean excep= false;
		WorkspaceBuffer.Element elem= null;
		try
		{
			elem= FrameworkFactory.getInstance().getTaskController().startMapRun(ID, key, partStart, partEnd);
			
			// System.out.println("run compiled command" + ID);
			elem.ws.runCompiledCommands(elem.owner, elem.map);
			excep= elem.ws.lastLogoException() != null;
			System.out.println("done compiled command " + ID + ": " + excep);
			
			returned= true;
		} catch (RuntimeException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExtensionException e) {
			e.printStackTrace();
		} finally {
			boolean suc = returned && !excep;
			if( excep )
				elem.ws.clearLastLogoException();
			try {
				FrameworkFactory.getInstance().getTaskController().setMapFinished(ID, suc, elem, key, partStart, partEnd);
			} catch (ExtensionException e) {
				e.printStackTrace();
				return false;
			}
		}
		
		return true;
	}
}
