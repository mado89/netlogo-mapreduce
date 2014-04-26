package at.dobiasch.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.nlogo.api.CompilerException;
import org.nlogo.api.ExtensionException;
import org.nlogo.extensions.mapreduce.Manager;

import at.dobiasch.mapreduce.framework.Framework;
import at.dobiasch.mapreduce.framework.InputChecker;
import at.dobiasch.mapreduce.framework.WorkspaceBuffer;
import at.dobiasch.mapreduce.framework.controller.HostController;
import at.dobiasch.mapreduce.framework.partition.CheckPartData;
import at.dobiasch.mapreduce.framework.task.IntKeyVal;

public class SingleNodeRun extends MapReduceRun
{
	String world;
	String modelpath;
	int size;
	WorkspaceBuffer wb;
	CheckPartData indata;
	
	public SingleNodeRun(Framework fw, String modelpath)
	{
		this.fw= fw;
		this.fw.setMaster(true);
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
				this.modelpath);
		
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
	
	public void run() throws ExtensionException
	{
		try
		{
			doMap();
			
			doReduce();
			
			outputResult();
		} catch (IOException e) {
			e.printStackTrace();
			throw new ExtensionException(e);
		} catch (CompilerException e) {
			e.printStackTrace();
			throw new ExtensionException(e);
		}
	}
	
	/**
	 * 
	 * @throws Exception
	 */
	private void prepareInput() throws Exception
	{
		InputChecker c= new InputChecker(fw);
		c.check();
		this.indata= c.getData();
	}
	
	private void prepareMapper() throws IOException, CompilerException
	{
		world= Manager.getWorld();
		// System.out.println("Prepare Mapper" + world);
		this.controller.prepareMappingStage(world);
	}
	
	private void doMap() throws IOException, ExtensionException
	{
		long partStart;
		long partEnd;
		for(at.dobiasch.mapreduce.framework.partition.Data data : indata.values())
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
				if( (partStart + 1) < partEnd )
					this.controller.addMap(this.controller.getID(), data.key, partStart, partEnd);
				
				partStart= partEnd;
			}
			partEnd= data.lastpartitionend;
			in.close();
			
			// Partitioning might have written last partition correct ;)
			// so don't add an empty task
			if( partStart < partEnd )
				this.controller.addMap(this.controller.getID(), data.key, partStart, partEnd);
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
		System.out.println(intdata);
		String[] kk= new String[intdata.size()];
		intdata.keySet().toArray(kk);
		// String key;
		// IntKeyVal value;
		
		System.out.println("Sorting");
		Arrays.sort(kk);
		
		System.out.println("Begin Reducing #Reduce-Tasks: " + kk.length);
		this.controller.prepareReduceStage();
		
		for(int i= 0; i < kk.length; i++)
		{
			this.controller.addReduce(this.controller.getID(), kk[i], intdata.get(kk[i]));
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

	/*private void doCollect() throws IOException, InterruptedException
	{
		String fn;
		System.out.println("Writing output");
		fn= this.fw.getConfiguration().getOutputDirectory();
		if( !fn.equals("") && !fn.endsWith(File.separator)) //TODO: make it work for OS-Problems \\ on linux
			fn+= File.separator;
		fn+= "output.txt";
		this.controller.mergeReduceOutput(fn);
	}*/
	
	@Override
	public double getMapProgress() {
		return this.controller.getMapProgress();
	}

	@Override
	public double getReduceProgress() {
		return this.controller.getReduceProgress();
	}

}
