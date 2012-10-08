package at.dobiasch.mapreduce.framework.controller;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.nlogo.api.CompilerException;
import org.nlogo.workspace.AbstractWorkspace;

import at.dobiasch.mapreduce.framework.Counter;
import at.dobiasch.mapreduce.framework.RecordWriterBuffer;
import at.dobiasch.mapreduce.framework.SysFileHandler;
import at.dobiasch.mapreduce.framework.WorkspaceBuffer;
import at.dobiasch.mapreduce.framework.WorkspaceBuffer.Element;
import at.dobiasch.mapreduce.framework.task.IntKeyVal;
import at.dobiasch.mapreduce.framework.task.MapRun;
import at.dobiasch.mapreduce.framework.task.ReduceRun;

public class HostController
{
	private WorkspaceBuffer wbmap;
	private WorkspaceBuffer wbred;
	// WorkspaceBuffer wbpart;
	
	private int mapc;
	private int redc;
	private String mapper;
	private String reducer;
	private ExecutorService pool;
	private ExecutorCompletionService<Object> complet;
	private Counter maptaskC;
	private Counter redtaskC;
	private String world;
	private String modelpath;
	
	private HostTaskController htc;
	private SysFileHandler sysh;
	private RecordWriterBuffer mapwriter;
	private RecordWriterBuffer reducewriter;
	private int _ID;
	
	/**
	 * 
	 * @param mapc Number of Mappers
	 * @param redc Number of Reducers
	 * @param mapn Name of the Mapper (in the source code of the model)
	 * @param redn Name of the Reducer (in the source code of the model)
	 */
	public HostController(int mapc, int redc, String mapn, String redn, 
			SysFileHandler sysh, String world, String modelpath)
	{
		this.mapc= mapc;
		this.redc= redc;
		this.mapper= mapn;
		this.reducer= redn;
		this.maptaskC= new Counter();
		this.redtaskC= new Counter();
		this.world= world;
		this.modelpath= modelpath;
		this.sysh= sysh;
		this.htc= new HostTaskController(sysh);
	}
	
	public void prepareMappingStage() throws IOException, CompilerException
	{
		this.wbmap= new WorkspaceBuffer(this.mapc ,world, modelpath);
		this.wbmap.compileComands(mapper, null);
		this.pool= Executors.newFixedThreadPool(this.mapc);
		this.complet= new ExecutorCompletionService<Object>(pool);
		
		this.mapwriter= new RecordWriterBuffer(this.mapc + 2, "map-%04d", this.sysh, " \t ");
		this.htc.setMapperOutput(this.mapwriter);
	}
	
	public void prepareReduceStage() throws IOException, CompilerException
	{
		this.wbred= new WorkspaceBuffer(this.mapc ,world, modelpath);
		this.wbred.compileComands(null, reducer);
		this.pool= Executors.newFixedThreadPool(this.redc);
		this.complet= new ExecutorCompletionService<Object>(pool);
		
		this.reducewriter= new RecordWriterBuffer(this.redc, "output-%04d", this.sysh, ": ");
		this.htc.setReduceOutput(this.reducewriter);
	}
	
	/**
	 * 
	 * @param ID
	 * @param key
	 * @param start
	 * @param end
	 */
	public long addMap(String key, long start, long end)
	{
		long ID= getID();
		System.out.println(ID + ": " + key + " " + start + " " + end + " submit");
		
		this.complet.submit(new MapRun(ID,key,start,end));
		
		this.maptaskC.add();
		return ID;
	}
	
	private synchronized long getID()
	{
		return this._ID++;
	}

	public long addReduce(String key, IntKeyVal value)
	{
		long ID= getID();
		this.complet.submit(new ReduceRun(ID, key, value));
		System.out.println("Submitted " + ID);
		this.redtaskC.add();
		
		return ID;
	}
	
	/**
	 * 
	 * @param ID the ID of the task to start
	 * @return a workspace in which the map-task can run
	 * @throws InterruptedException 
	 */
	public WorkspaceBuffer.Element startMapRun(long ID, String key, long start, long end) throws InterruptedException
	{
		WorkspaceBuffer.Element elem= wbmap.get();
		this.htc.addMap(elem.ws, ID, key, start, end);
		return elem;
	}
	
	public void setMapFinished(long ID, boolean success, WorkspaceBuffer.Element elem, String key, long start, long end)
	{
		// System.out.println("Map " + ID + "finished : " + success);
		try {
			this.htc.removeMap(elem.ws, success);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		wbmap.release(elem);
		if( success == false )
		{
			this.addMap(key, start, end);
		}
	}
	
	public boolean waitForMappingStage()
	{
		try
		{
			// System.out.println("Jobs submitted wait for shutdown");
			// pool.shutdown();
			// ---> don't shut down. Otherwise it could cause problems
			for(int l= 0; l < this.maptaskC.getValue(); l++)
			{
				System.out.println("Try to take " + l + " " + this.maptaskC.getValue());
				if( (Boolean) complet.take().get() != true)
				{
					System.out.println("Something failed");
				}
			}
			System.out.println("All taken");
		} catch (InterruptedException e) {
			System.out.println("Waiting for Map-Tasks was interruped");
			return false;
		} catch (ExecutionException e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}

	public void finishMappingStage() {
		// try {
			// htc.closeIntermediateFiles();
			wbmap.dispose();
		// } catch (IOException e) {
		// 	System.out.println("IO Except");
		// 	e.printStackTrace();
		// }
	}

	public Map<String, IntKeyVal> getIntermediateData()
	{
		return this.htc.getIntermediateData();
	}

	/**
	 * 
	 * @param ID the ID of the task to start
	 * @return a workspace in which the map-task can run
	 * @throws InterruptedException 
	 */
	public Element startReduceRun(long ID, String key, String filename, long fileSize) throws InterruptedException
	{
		WorkspaceBuffer.Element elem= wbred.get();
		this.htc.addReduce(elem.ws, ID, key, filename, fileSize);
		return elem;
	}

	public void setReduceFinished(long iD, boolean success, Element elem, String key, IntKeyVal value)
	{
		// this.htc.removeReduce(elem.ws);
		// wbred.release(elem);
		System.out.println("Reduce " + iD + "finished : " + success);
		try {
			this.htc.removeReduce(elem.ws, success);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		wbred.release(elem);
		if( success == false )
		{
			this.addReduce(key, value);
		}
	}
	
	public boolean waitForReduceStage()
	{
		
		try
		{
			System.out.println("Jobs submitted wait for shutdown");
			// pool.shutdown();
			// ---> don't shut down. Otherwise it could cause problems
			for(int l= 0; l < this.redtaskC.getValue(); l++)
			{
				System.out.println("Try to take " + l + " " + this.redtaskC.getValue());
				if( (Boolean) complet.take().get() != true)
				{
					System.out.println("Something failed");
				}
			}
			System.out.println("All taken --> Reduce done");
		} catch (InterruptedException e) {
			System.out.println("Waiting for Reduce-Tasks was interruped");
			e.printStackTrace();
			return false;
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	public void finishReduceStage() throws IOException
	{
		reducewriter.closeAll();
		wbred.dispose();
		System.out.println("All reduce files are closed");
	}

	public void emit(AbstractWorkspace ws, String key, String value) throws IOException
	{
		this.htc.emit(ws, key, value);
	}

	public Data getData(AbstractWorkspace ws)
	{
		return this.htc.getData(ws);
	}

	public void mergeReduceOutput() throws IOException, InterruptedException
	{
		// TODO: implement me
		/*RecordReader in[];
		RecordWriter out;
		int size= this.reducewriter.getSize();
		boolean recsleft= true;
		
		in= new RecordReader[size];
		out= new RecordWriter("output.txt", this.reducewriter.getKeyValueSeperator());
		for(int i= 0; i < size; i++)
			in[i]= new RecordReader(this.reducewriter.get());
		
		while( recsleft )
		{
			in[i].
		}*/
	}
}
