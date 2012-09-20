package at.dobiasch.mapreduce.framework.controller;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.nlogo.api.CompilerException;
import org.nlogo.nvm.Workspace;

import at.dobiasch.mapreduce.framework.RecordWriter;
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
	private int maptaskC;
	private int redtaskC;
	private String world;
	private String modelpath;
	
	private HostTaskController htc;
	private FileWriter[] out;
	private SysFileHandler sysh;
	private RecordWriterBuffer mapwriter;
	
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
		this.maptaskC= 0;
		this.redtaskC= 0;
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
		
		this.mapwriter= new RecordWriterBuffer(this.mapc, "map-%04d", this.sysh, " \t ");
		this.htc.setMapperOutput(this.mapwriter);
	}
	
	public void prepareReduceStage() throws IOException, CompilerException
	{
		this.wbred= new WorkspaceBuffer(this.mapc ,world, modelpath);
		this.wbred.compileComands(null, reducer);
		this.pool= Executors.newFixedThreadPool(this.redc);
		this.complet= new ExecutorCompletionService<Object>(pool);
		
		this.mapwriter= new RecordWriterBuffer(this.mapc, "output-%04d", this.sysh, ": ");
		this.htc.setReduceOutput(this.mapwriter);
	}
	
	/**
	 * 
	 * @param ID
	 * @param key
	 * @param start
	 * @param end
	 */
	public void addMap(long ID, String key, long start, long end)
	{
		System.out.println(ID + ": " + key + " " + start + " " + end + " submit");
		
		this.complet.submit(new MapRun(ID,key,start,end));
		
		this.maptaskC++;
	}
	
	public void addReduce(long ID, String key, IntKeyVal value)
	{
		this.complet.submit(new ReduceRun(ID, key, value));
		System.out.println("Submitted " + ID);
		this.redtaskC++;
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
	
	public void setMapFinished(long ID, boolean success, WorkspaceBuffer.Element elem)
	{
		try {
			this.htc.removeMap(elem.ws, success);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		wbmap.release(elem);
		
	}
	
	public boolean waitForMappingStage()
	{
		try
		{
			System.out.println("Jobs submitted wait for shutdown");
			pool.shutdown();
			for(int l= 0; l < this.maptaskC; l++)
			{
				System.out.println("Try to take " + l + " " + this.maptaskC);
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
		try {
			htc.closeIntermediateFiles();
			wbmap.dispose();
		} catch (IOException e) {
			System.out.println("IO Except");
			e.printStackTrace();
		}
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

	public void setReduceFinished(long iD, boolean returned, Element elem)
	{
		wbred.release(elem);
	}
	
	public boolean waitForReduceStage()
	{
		
		try
		{
			System.out.println("Jobs submitted wait for shutdown");
			this.pool.shutdown();
			for(int l= 0; l < this.redtaskC; l++)
			{
				System.out.println("Try to take " + l + " " + this.redtaskC);
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
		for(int i= 0; i < this.redc; i++)
		{
			this.out[i].close();
		}
		System.out.println("All reduce files are closed");
	}

	public void emit(Workspace ws, String key, String value) throws IOException
	{
		this.htc.emit(ws, key, value);
	}

	public Data getData(Workspace ws)
	{
		return this.htc.getData(ws);
	}
}
