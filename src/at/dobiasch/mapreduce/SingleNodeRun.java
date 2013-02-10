package at.dobiasch.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.nlogo.api.CompilerException;
import org.nlogo.api.ExtensionException;

import at.dobiasch.mapreduce.framework.Framework;
import at.dobiasch.mapreduce.framework.WorkspaceBuffer;
import at.dobiasch.mapreduce.framework.controller.HostController;
import at.dobiasch.mapreduce.framework.partition.ICheckAndPartition;
import at.dobiasch.mapreduce.framework.task.IntKeyVal;

public class SingleNodeRun extends MapReduceRun
{
	String world;
	String modelpath;
	int size;
	WorkspaceBuffer wb;
	Framework fw;
	Map<String,ICheckAndPartition.CheckPartData> indata;
	
	/*
	 * Number of input splits ie number of mappers that has to be run
	 
	private int ninpsplit;*/
	private HostController controller;
	
	public SingleNodeRun(Framework fw, String world, String modelpath)
	{
		this.fw= fw;
		this.fw.setMaster(true);
		this.world= world;
		this.modelpath= modelpath;
	}
	
	public void setup() throws ExtensionException
	{
		System.out.println("Setting up");
		// TODO: its assumed that working/system directory is created and write able
		
		this.controller= new HostController( fw.getConfiguration().getMappers(),
				fw.getConfiguration().getReducers(),
				fw.getConfiguration().getMapper(), 
				fw.getConfiguration().getReducer(),
				fw.getSystemFileHandler(), 
				this.world, this.modelpath);
		
		this.fw.setHostController(this.controller);
		
		try
		{
			prepareInput();
			prepareMapper();
		} catch (CompilerException e){
			throw new ExtensionException(e);
		} catch (IOException e) {
			throw new ExtensionException(e);
		} catch (Exception e) {
			throw new ExtensionException(e);
		}
	}
	
	protected void run() throws ExtensionException
	{
		try
		{
			doMap();
			
			doReduce();
			
			doCollect();
		} catch (IOException e) {
			throw new ExtensionException(e);
		} catch (CompilerException e) {
			throw new ExtensionException(e);
		} catch (InterruptedException e) {
			throw new ExtensionException(e);
		}
	}
	
	/**
	 * 
	 * @throws Exception
	 */
	private void prepareInput() throws Exception
	{
		ICheckAndPartition part= fw.getNewPartitioner();
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
		/*this.ninpsplit= 0;
		for(ICheckAndPartition.CheckPartData data : indata.values())
		{
			this.ninpsplit+= data.numpartitions;
		}*/
		
		System.out.println(indata);
	}
	
	private void prepareMapper() throws IOException, CompilerException
	{
		this.controller.prepareMappingStage();
	}
	
	private void doMap() throws IOException, ExtensionException
	{
		long partStart;
		long partEnd;
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
				// don't add an empty task
				if( partStart < partEnd )
					this.controller.addMap(data.key, partStart, partEnd);
				
				partStart= partEnd;
			}
			partEnd= data.lastpartitionend;
			
			// Partitioning might have written last partition correct ;)
			// so don't add an empty task
			if( partStart < partEnd )
				this.controller.addMap(data.key, partStart, partEnd);
		}
		
		boolean result= this.controller.waitForMappingStage();
		if( result == false)
			throw new ExtensionException("Mapping-Stage failed");
		
		this.controller.finishMappingStage();
		
		System.out.println("done mapping");
	}
	
	private void doReduce() throws IOException, CompilerException, ExtensionException
	{
		Map<String,IntKeyVal> intdata= this.controller.getIntermediateData();
		// Iterator<IntKeyVal> vals= intdata.values().iterator();
		// Iterator<String> keys= intdata.keySet().iterator();
		String[] kk= new String[intdata.size()];
		intdata.keySet().toArray(kk);
		// String key;
		// IntKeyVal value;
		
		System.out.println("Sorting");
		Arrays.sort(kk);
		
		System.out.println("Begin Reducing");
		this.controller.prepareReduceStage();
		
		for(int i= 0; i < kk.length; i++)
		{
			this.controller.addReduce(kk[i], intdata.get(kk[i]));
		}
		/*
		while(keys.hasNext())
		{
			key= keys.next();
			value= vals.next();
			this.controller.addReduce(key, value);
		}*/
		
		boolean result= this.controller.waitForReduceStage();
		if( result == false)
			throw new ExtensionException("Reduce-Stage failed");
		
		this.controller.finishReduceStage();
		System.out.println("Reducing ended");
	}

	private void doCollect() throws IOException, InterruptedException
	{
		String fn;
		System.out.println("Writing output");
		fn= this.fw.getConfiguration().getOutputDirectory();
		if( fn.equals("") )
			fn= "./"; // TODO: check if this works on windows
		else if( !fn.endsWith("" + File.separatorChar) ) //TODO: make it work for OS-Problems \\ on linux
			fn+= File.separatorChar;
		fn+= "output.txt";
		this.controller.mergeReduceOutput(fn);
	}

	@Override
	public double getMapProgress() {
		return this.controller.getMapProgress();
	}

	@Override
	public double getReduceProgress() {
		return this.controller.getReduceProgress();
	}
}
