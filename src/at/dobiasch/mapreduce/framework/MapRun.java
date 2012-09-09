package at.dobiasch.mapreduce.framework;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.util.concurrent.Callable;

import org.nlogo.api.AgentException;
import org.nlogo.api.LogoException;
import org.nlogo.api.LogoList;
import org.nlogo.api.LogoListBuilder;

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
			// FileChannel fc = FileChannel.open(key, StandardOpenOption.READ);
			RandomAccessFile in= new RandomAccessFile(key, "r");
			byte[] b= new byte[(int) (partEnd - partStart)];
			
			in.seek(partStart);
			in.read(b);
			
			elem= wb.get();
			
			LogoListBuilder list = new LogoListBuilder();
			String[] vals= new String(b).split("\n");
			b= null;
			for(int i= 0; i < vals.length; i++)
				list.add(vals[i]);
			
			System.out.println("set values " + ID);
			// elem.ws.world.observer().
			// elem.ws.world.setObserverVariableByName("mapreduce.values", list.toLogoList());
			
			System.out.println("run compiled command");
			elem.ws.runCompiledCommands(elem.owner, elem.map);
			
			wb.release(elem);
			returned= true;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} /* catch (AgentException e) {
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
		
		return null;
	}
}
