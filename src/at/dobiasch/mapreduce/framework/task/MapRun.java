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
	WorkspaceBuffer wb;
	
	public MapRun(long ID, String key, long partStart, long partEnd, WorkspaceBuffer wb)
	{
		this.ID= ID;
		this.key= key;
		this.partStart= partStart;
		this.partEnd= partEnd;
		this.wb= wb;
	}

	@Override
	public Object call()
	{
		boolean returned= false;
		WorkspaceBuffer.Element elem= null;
		try
		{
			// System.out.println("set values " + ID);
			// elem.ws.world.observer().
			// elem.ws.world.setObserverVariableByName("mapreduce.values", list.toLogoList());
			
			elem= wb.get();
			FrameworkFactory.getInstance().getTaskController().addMap(elem.ws, ID, key, partStart, partEnd);
			System.out.println("run compiled command" + ID);
			System.out.println(elem.ws.runCompiledCommands(elem.owner, elem.map));
			System.out.println("done compiled command" + ID);
			FrameworkFactory.getInstance().getTaskController().removeMap(elem.ws);
			
			wb.release(elem);
			System.out.println("Released WB");
			returned= true;
		} /* catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} *//* catch (AgentException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} catch (LogoException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} */ catch (InterruptedException e) {
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
