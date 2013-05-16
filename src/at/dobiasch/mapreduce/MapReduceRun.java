package at.dobiasch.mapreduce;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.nlogo.api.ExtensionException;

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
}
