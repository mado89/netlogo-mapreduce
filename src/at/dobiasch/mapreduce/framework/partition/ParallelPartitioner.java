package at.dobiasch.mapreduce.framework.partition;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import at.dobiasch.mapreduce.framework.FrameworkException;

public class ParallelPartitioner implements ICheckAndPartition
{
	private Map<String,String> files;
	// private boolean syncMapwait= false;
	
	private ExecutorService pool;
	private CompletionService<Object> complet;
	
	private int jobCount;
	
	@Override
	public void init(String sysdir)
	{
		files= new HashMap<String,String>();
		
		// TODO: maybe find a better number of threads
		pool= Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		complet= new ExecutorCompletionService<Object>(pool);
		jobCount= 0;
	}
	
	@Override
	public void addFile(String path)
	{
		try {
			complet.submit( new BaseParallelPartitioner(path, "/tmp") );
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		jobCount++;
	}
	
	@Override
	public Map<String, String> getChecksums() throws FrameworkException
	{
		try
		{
			pool.shutdown();
			for(int l= 0; l < jobCount; l++)
			{
				String ret[];
				ret= (String[] ) complet.take().get();
				files.put(ret[0], ret[1]);
			}
		} catch(InterruptedException e) {
			throw new FrameworkException( e.getMessage() );
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		
		return files;
	}
}
