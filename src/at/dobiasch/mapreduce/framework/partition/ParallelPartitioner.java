package at.dobiasch.mapreduce.framework.partition;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import at.dobiasch.mapreduce.framework.FrameworkException;
import at.dobiasch.mapreduce.framework.SysFileHandler;

public class ParallelPartitioner implements ICheckAndPartition
{
	// private Map<String,String> filesC;
	// private Map<String,CheckPartData> filesD;
	private CheckPartData files;
	// private boolean syncMapwait= false;
	
	private ExecutorService pool;
	private CompletionService<Object> complet;
	
	private Class partitioner= ParallelLinePartitioner.class;
	private Constructor<BaseParallelPartitioner> constr;
	
	private int jobCount;

	private int blocksize;
	
	protected boolean check;
	protected SysFileHandler sysfileh;
	
	@Override
	public void init(SysFileHandler sysfileh, int blocksize) throws SecurityException, NoSuchMethodException
	{
		this.blocksize= blocksize;
		this.check= true;
		this.sysfileh= sysfileh;
		
		// filesC= null;
		// filesD= null;
		files= null;
		
		// TODO: maybe find a better number of threads
		pool= Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		complet= new ExecutorCompletionService<Object>(pool);
		jobCount= 0;
		
		@SuppressWarnings("rawtypes")
		Class[] ctorArgs1 = new Class[3];
		ctorArgs1[0] = String.class;
		ctorArgs1[1] = SysFileHandler.class;
		ctorArgs1[2] = Integer.class;
        constr= partitioner.getConstructor(ctorArgs1);
	}
	
	@Override
	public void addFile(String path)
	{
		try {
			complet.submit( constr.newInstance(path, sysfileh, blocksize) );
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
	
	// @Override
	/*public Map<String, String> getChecksums() throws FrameworkException
	{
		//filesC= new HashMap<String,String>();
		files= new CheckPartData();
		
		try
		{
			pool.shutdown();
			for(int l= 0; l < jobCount; l++)
			{
				Data ret;
				ret= (Data ) complet.take().get();
				files.putChecksum(ret.key, ret.checksum);
			}
		} catch(InterruptedException e) {
			throw new FrameworkException( e.getMessage() );
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		
		return files.getChecksums();
	}*/

	@Override
	public void setCheck(boolean check)
	{
		this.check= check;
	}

	@Override
	public CheckPartData getData() throws FrameworkException
	{
		files= new CheckPartData();
		try
		{
			pool.shutdown();
			for(int l= 0; l < jobCount; l++)
			{
				Data ret;
				ret= (Data ) complet.take().get();
				files.put(ret.key, ret);
			}
		} catch(InterruptedException e) {
			throw new FrameworkException( e.getMessage() );
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		
		return files;
	}
}
