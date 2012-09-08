package at.dobiasch.mapreduce.framework.partition;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
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
	
	private Class partitioner= ParallelLinePartitioner.class;
	private Constructor<BaseParallelPartitioner> constr;
	
	private int jobCount;

	private int blocksize;
	
	@Override
	public void init(String sysdir, int blocksize) throws SecurityException, NoSuchMethodException
	{
		this.blocksize= blocksize;
		
		files= new HashMap<String,String>();
		
		// TODO: maybe find a better number of threads
		pool= Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		complet= new ExecutorCompletionService<Object>(pool);
		jobCount= 0;
		
		@SuppressWarnings("rawtypes")
		Class[] ctorArgs1 = new Class[3];
		ctorArgs1[0] = String.class;
		ctorArgs1[1] = String.class;
		ctorArgs1[2] = Integer.class;
        constr= partitioner.getConstructor(ctorArgs1);
	}
	
	@Override
	public void addFile(String path)
	{
		try {
			complet.submit( constr.newInstance(path, "/tmp", blocksize) );
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
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
