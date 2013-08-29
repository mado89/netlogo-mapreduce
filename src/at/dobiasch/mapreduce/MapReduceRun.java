package at.dobiasch.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.nlogo.api.ExtensionException;

import at.dobiasch.mapreduce.framework.Framework;
import at.dobiasch.mapreduce.framework.controller.HostController;
import at.dobiasch.mapreduce.framework.partition.CheckPartData;

public abstract class MapReduceRun {
	
	private class RunHelper implements Callable<Object> {
		
		private MapReduceRun caller;
		public RunHelper(MapReduceRun caller)
		{
			this.caller= caller;
		}

		@Override
		public Object call() throws Exception {
			try{
				caller.run();
				caller.setFinished();
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println(e.getMessage());
				throw e;
			}
			return null;
		}
		
	}

	private ExecutorService pool;
	private ExecutorCompletionService<Object> complet;
	private boolean running;
	protected Framework fw;
	protected HostController controller;
	
	public MapReduceRun()
	{
		this.pool= Executors.newSingleThreadExecutor();
		this.complet= new ExecutorCompletionService<Object>(pool);
		this.running= false;
	}
	
	
	// abstract double getProgress();
	public abstract double getMapProgress();
	public abstract double getReduceProgress();
	
	public synchronized boolean isRunning()
	{
		return this.running;
	}
	
	public void startRun()
	{
		this.running= true;
		this.complet.submit(new RunHelper(this));
	}
	
	protected abstract void run() throws ExtensionException;
	
	private void setFinished()
	{
		this.running= false;
	}


	public abstract void setup() throws ExtensionException;
	
	protected void outputResult() throws ExtensionException
	{
		String fn;
		System.out.println("Writing output");
		fn= this.fw.getConfiguration().getOutputDirectory();
		if( !fn.equals("") && !fn.endsWith(File.separator)) //TODO: make it work for OS-Problems \\ on linux
			fn+= File.separator;
		fn+= String.format("output-%02d.txt", fw.getJobNr());
		try {
			this.controller.mergeReduceOutput(fn);
		} catch (IOException e) {
			e.printStackTrace();
			throw new ExtensionException(e);
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new ExtensionException(e);
		}
	}
}
