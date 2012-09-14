package at.dobiasch.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.nlogo.api.CompilerException;
import org.nlogo.api.ExtensionException;

import at.dobiasch.mapreduce.framework.Framework;
import at.dobiasch.mapreduce.framework.task.IntKeyVal;
import at.dobiasch.mapreduce.framework.task.MapRun;
import at.dobiasch.mapreduce.framework.task.ReduceRun;
import at.dobiasch.mapreduce.framework.WorkspaceBuffer;
import at.dobiasch.mapreduce.framework.partition.ICheckAndPartition;
import at.dobiasch.mapreduce.framework.partition.ParallelPartitioner;

public class SingleNodeRun
{
	String world;
	String modelpath;
	int size;
	WorkspaceBuffer wb;
	Framework fw;
	Map<String,ICheckAndPartition.CheckPartData> indata;
	private ExecutorService pool;
	private ExecutorCompletionService<Object> complet;
	
	/**
	 * Number of input splits ie number of mappers that has to be run
	 */
	private int ninpsplit;
	
	public SingleNodeRun(Framework fw, String world, String modelpath)
	{
		this.fw= fw;
		this.world= world;
		this.modelpath= modelpath;
	}
	
	public void setup() throws ExtensionException
	{
		// TODO: rename this --> setup does everything?
		System.out.println("Setting up");
		// TODO: its assumed that working/system directory is created and write able
		try
		{
			prepareInput();
			prepareMapper();
			doMap();
			
			doReduce();
			
		} catch (CompilerException e1){
			throw new ExtensionException(e1);
		} catch (IOException e) {
			throw new ExtensionException(e);
		} catch (Exception e) {
			throw new ExtensionException(e);
		}
	}
	
	/**
	 * 
	 * @throws Exception
	 */
	private void prepareInput() throws Exception
	{
		ICheckAndPartition part= new ParallelPartitioner();
		part.init(fw.getSystemFileHandler(), 250);
		part.setCheck(false);
		
		String indir= fw.getConfiguration().getInputDirectory();
		File inputdir = new File(indir);
		
		FilenameFilter filter = new FilenameFilter() {
		    public boolean accept(File dir, String name) {
		        return !name.startsWith(".");
		    }
		};

		String[] children = inputdir.list(filter);
		if (children == null)
		{
		    // Either dir does not exist or is not a directory
		}
		else
		{
		    for (int i=0; i<children.length; i++) {
		        // Get filename of file or directory
		    	System.out.println(children[i]);
		        part.addFile(indir + "/" + children[i]);
		    }
		}
		
		indata= part.getData();
		this.ninpsplit= 0;
		for(ICheckAndPartition.CheckPartData data : indata.values())
		{
			this.ninpsplit+= data.numpartitions;
		}
		
		System.out.println(indata);
	}
	
	private void prepareMapper() throws IOException, CompilerException
	{
		this.size= fw.getConfiguration().getMappers();
		this.wb= new WorkspaceBuffer(size ,world, modelpath);
		this.wb.compileComands(fw.getConfiguration().getMapper(), fw.getConfiguration().getReducer());
		this.pool= Executors.newFixedThreadPool(this.size);
		this.complet= new ExecutorCompletionService<Object>(pool);
	}
	
	private void doMap() throws IOException
	{
		long partStart;
		long partEnd;
		int i= 0;
		for(ICheckAndPartition.CheckPartData data : indata.values())
		{
			File file= new File(data.partitionfile);
			BufferedReader in= new BufferedReader(new FileReader(file));
			
			String line;
			line= in.readLine(); //TODO: this assumes there is a line ... not good
			partStart= Integer.parseInt(line);
			while((line= in.readLine()) != null)
			{
				partEnd= Integer.parseInt(line);
				System.out.println(i + ": " + data.key + " " + partStart + " " + partEnd + " submit");
				this.complet.submit(new MapRun(i, data.key, partStart, partEnd, wb));
				i++;
				partStart= partEnd;
			}
			partEnd= data.lastpartitionend;
			System.out.println(i + ": " + data.key + " " + partStart + " " + partEnd + " submit");
			this.complet.submit(new MapRun(i, data.key, partStart, partEnd, wb));
			i++;
		}
		
		try
		{
			System.out.println("Jobs submitted wait for shutdown");
			pool.shutdown();
			for(int l= 0; l < i; l++)
			{
				System.out.println("Try to take " + l + " " + i);
				if( (Boolean) complet.take().get() != true)
				{
					System.out.println("Something failed");
				}
			}
			System.out.println("All taken");
		} catch (InterruptedException e) {
			System.out.println("Waiting for Map-Tasks was interruped");
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			fw.getTaskController().closeIntermediateFiles();
		} catch (IOException e) {
			System.out.println("IO Except");
			e.printStackTrace();
		}
		System.out.println("done mapping");
	}
	
	private void doReduce() throws IOException
	{
		Map<String,IntKeyVal> intdata= fw.getTaskController().getIntermediateData();
		Iterator<IntKeyVal> vals= intdata.values().iterator();
		Iterator<String> keys= intdata.keySet().iterator();
		String key;
		IntKeyVal value;
		int i= 0;
		
		System.out.println("Begin Reducing");
		
		// Resize the Buffer
		this.size= fw.getConfiguration().getReducers();
		wb.resize(this.size);
		
		this.pool= Executors.newFixedThreadPool(this.size);
		this.complet= new ExecutorCompletionService<Object>(pool);
		
		FileWriter[] out= new FileWriter[this.size];
		for(i= 0; i < this.size; i++)
		{
			//TODO: format 0 --> 0000, 1 --> 0001
			out[i]= new FileWriter("output-" + i);
		}
		fw.getTaskController().setReduceOutput(out);
		
		i= 0;
		while(keys.hasNext())
		{
			key= keys.next();
			value= vals.next();
			this.complet.submit(new ReduceRun(i, key, value, wb));
			System.out.println("Submitted " + i);
			i++;
		}
		
		try
		{
			System.out.println("Jobs submitted wait for shutdown");
			this.pool.shutdown();
			for(int l= 0; l < i; l++)
			{
				System.out.println("Try to take " + l + " " + i);
				if( (Boolean) complet.take().get() != true)
				{
					System.out.println("Something failed");
				}
			}
			System.out.println("All taken --> Reduce done");
		} catch (InterruptedException e) {
			System.out.println("Waiting for Reduce-Tasks was interruped");
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		for(i= 0; i < this.size; i++)
		{
			out[i].close();
		}
		
		System.out.println("Reducing ended");
	}

}
