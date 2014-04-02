package at.dobiasch.mapreduce.framework.task;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.Callable;

import org.nlogo.api.ExtensionException;

import at.dobiasch.mapreduce.framework.LogoObject;
import at.dobiasch.mapreduce.framework.Framework;
import at.dobiasch.mapreduce.framework.FrameworkFactory;
import at.dobiasch.mapreduce.framework.WorkspaceBuffer;
import at.dobiasch.mapreduce.framework.controller.Data;
import at.dobiasch.mapreduce.framework.controller.HostController;

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
			Framework fw= FrameworkFactory.getInstance();
			HostController controller= fw.getTaskController();
			elem= controller.startReduceRun(ID, key, value.fn, value.getFileSize(),partition);
			
			// elem.ws.runCompiledCommands(elem.owner, elem.read);
			// elem.ws.runCompiledCommands(elem.owner, elem.reduce);
			// elem.ws.runCompiledCommands(elem.owner, elem.clean);
			Data data= controller.getData(elem.ws);
			
			try
			{
				RandomAccessFile in = new RandomAccessFile(data.src, "r");
				byte[] b= new byte[(int) (data.end - data.start)];
				
				in.seek(data.start);
				in.read(b);
				
				in.close();
				
				String[] vals= new String(b).split("\n");
				
				System.out.println("read " + data.ID + " " + data.key + " " + vals[0].replaceAll("\\n","") + " " + vals.length);
				LogoObject accum= fw.getConfiguration().getAccumulator().copy();
				
				int count= 0;
				boolean success= false;
				for(int i= 0; i < vals.length;)
				{
					String cmd= controller.getReducer() + " \"" + data.key + "\" " + accum.toLogo() + " \"" + vals[i] + "\"";
					System.out.println(cmd);
					Object o= null;
					try
					{
						o= elem.ws.report(cmd);
						System.out.println("Reduce step returned: " + o);
						success= elem.ws.lastLogoException() == null;
					} catch( Exception e) {
						System.out.println("Exception caught in reporter");
						if( elem.ws.lastLogoException() != null )
						{
							System.out.println("LogoException: " + elem.ws.lastLogoException().getMessage());
						}
						e.printStackTrace();
						success= false;
					}
					if( success )
					{
						accum.set(o);
						i++;
						count= 0;
					}
					else
					{
						if( elem.ws.lastLogoException() != null )
							System.out.println(elem.ws.lastLogoException().getMessage());
						count++;
						if( count > 1 ) //TODO: this needs to be logged!
						{
							System.out.println("Reduce-Step failed -> skip");
							
							i++;
							count= 0;
						}
						else
							System.out.println("Reduce-Step failed");
					}
				}
				
				/**
				 * Need to use export otw lists are not handled right
				 */
				// System.out.println("Finished: " + accum.export().substring(1) + " vs " + accum.toString());
				
				controller.emit(elem.ws, data.key, accum.export());
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				throw new ExtensionException(e);
			} catch (IOException e) {
				e.printStackTrace();
				throw new ExtensionException(e);
			}
			
			excep= elem.ws.lastLogoException() != null;
			System.out.println("done compiled command " + ID + ": " + excep);
			
			returned= true;
		} catch (InterruptedException e) {
			System.out.println(e.getMessage());
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
