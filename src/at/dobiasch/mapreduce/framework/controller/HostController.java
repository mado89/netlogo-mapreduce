package at.dobiasch.mapreduce.framework.controller;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.nlogo.api.CompilerException;
import org.nlogo.workspace.AbstractWorkspace;

import at.dobiasch.mapreduce.framework.Counter;
import at.dobiasch.mapreduce.framework.RecordReader;
import at.dobiasch.mapreduce.framework.RecordWriter;
import at.dobiasch.mapreduce.framework.RecordWriterBuffer;
import at.dobiasch.mapreduce.framework.SysFileHandler;
import at.dobiasch.mapreduce.framework.WorkspaceBuffer;
import at.dobiasch.mapreduce.framework.WorkspaceBuffer.Element;
import at.dobiasch.mapreduce.framework.partition.HashPartitioner;
import at.dobiasch.mapreduce.framework.partition.IPartitioner;
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
	private ExecutorService redpool[];
	private ExecutorCompletionService<Object> redcomplet[];
	private Counter maptaskC;
	private Counter redtaskC;
	private String world;
	private String modelpath;
	
	private HostTaskController htc;
	private SysFileHandler sysh;
	private RecordWriterBuffer mapwriter;
	private ConcurrentHashMap<Integer, RecordWriter> reducewriter;
	private int _ID;
	private RecordWriter redparts;
	
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
	
	@SuppressWarnings("unchecked") //TODO: this is for redcomplet init ...
	public void prepareReduceStage() throws IOException, CompilerException
	{
		this.wbred= new WorkspaceBuffer(this.mapc ,world, modelpath);
		this.wbred.compileComands(null, reducer);
		// TODO: belows null assignments are only for debugging, remove them
		this.complet= null;
		this.pool= null;
		this.redpool= new ExecutorService[this.redc];
		this.redcomplet= (ExecutorCompletionService<Object>[]) Array.newInstance(ExecutorCompletionService.class, this.redc); // new ExecutorCompletionService<Object>[this.redc];
		
		reducewriter= new ConcurrentHashMap<Integer,RecordWriter>();
		for(int i= 0; i < this.redc; i++)
		{
			ExecutorService pool= Executors.newFixedThreadPool(1);
			this.redpool[i]= pool;
			this.redcomplet[i]= new ExecutorCompletionService<Object>(pool);
			RecordWriter w;
			w= new RecordWriter(sysh.addFile(String.format("output-%04d", i)), ": ");
			w.writeSessionInfo(true);
			reducewriter.put(i, w);
		}
		// this.reducewriter= new RecordWriterBuffer(this.redc, "output-%04d", this.sysh, ": ");
		this.htc.setReduceOutput(this.reducewriter);
		// this.redparts= new HashMap<String,Integer>();
		this.redparts= new RecordWriter(sysh.addFile("keyparts.rec"),": ");
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
		IPartitioner part= new HashPartitioner();
		int p= part.getPartition(key, null, this.redc);
		this.redcomplet[p].submit(new ReduceRun(ID, key, value,p));
		System.out.println("Submitted " + ID + " " + p);
		this.redtaskC.add();
		
		// this.redparts.put(key, p);
		try {
			this.redparts.write(key, "" + p);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
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
	 * @param partition 
	 * @return a workspace in which the map-task can run
	 * @throws InterruptedException 
	 */
	public Element startReduceRun(long ID, String key, String filename, long fileSize, int partition) throws InterruptedException
	{
		WorkspaceBuffer.Element elem= wbred.get();
		this.htc.addReduce(elem.ws, ID, key, filename, fileSize,partition);
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
			System.out.println(iD);
			e.printStackTrace();
		}
		
		wbred.release(elem);
		if( success == false )
		{
			this.addReduce(key, value);
		}
		
		// TODO: fuer den ultimate fail eines reduce tasks: 
		// auch beachten: redparts muss adaptiert werden
	}
	
	public boolean waitForReduceStage()
	{
		class Waiter extends Thread
		{
			ExecutorCompletionService<Object> comps;
			Counter collected;
			boolean fail;
			
			public Waiter(ExecutorCompletionService<Object> comps, Counter collected)
			{
				super();
				this.comps= comps;
				this.collected= collected;
				this.fail= false;
			}
			
			public void run()
			{
				try
				{
					/*while(collected.getValue() < redtaskC.getValue())
					{
						if( (Boolean) comps.take().get() != true)
						{
							System.out.println("Something failed");
						}
						collected.add();
					}*/
					while(collected.getValue() < redtaskC.getValue())
					{
						Future<Object> f= comps.poll(1, TimeUnit.SECONDS);
						if( f != null )
						{
							if( (Boolean) f.get() != true)
							{
								System.out.println("Something failed");
							}
							collected.add();
						}
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
					this.fail= true;
				} catch (ExecutionException e) {
					e.printStackTrace();
					this.fail= true;
				}
			}

			public boolean fail()
			{
				return this.fail;
			}
		}
		
		try
		{
			System.out.println("Jobs submitted wait for shutdown");
			// pool.shutdown();
			// ---> don't shut down. Otherwise it could cause problems
			Counter rc= new Counter();
			Thread[] t= new Thread[this.redc];
			// for(int l= 0; l < this.redtaskC.getValue(); l++)
			for(int l= 0; l < this.redc; l++)
			{
				t[l]= new Waiter(this.redcomplet[l], rc);
				t[l].start();
				/*System.out.println("Try to take " + l + " " + this.redtaskC.getValue());
				if( (Boolean) complet.take().get() != true)
				{
					System.out.println("Something failed");
				}*/
			}
			for(int l = 0; l < this.redc; l++)
			{
		        t[l].join();
		        if( ((Waiter)t[l]).fail() )
		        {
		        	System.out.println("Error, waiting for reducers failed");
		        	break;
		        }
			}
			System.out.println("All taken --> Reduce done");
		} catch (InterruptedException e) {
			System.out.println("Waiting for Reduce-Tasks was interruped");
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	public void finishReduceStage() throws IOException
	{
		System.out.println("finish reduce stage");
		// reducewriter.closeAll();
		/*for(int i= 0; i < this.redc; i++)
		{
			RecordWriter w= reducewriter.get(i);
			if( w.isOpen() )
				w.close();
		}*/
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

	public void mergeReduceOutput(String fn) throws IOException, InterruptedException
	{
		RecordReader[] reader;
		RecordWriter out;
		RecordReader in;
		int i;
		
		reader= new RecordReader[this.redc];
		out= new RecordWriter(fn, ": ");
		in= new RecordReader(this.redparts);
		
		for(i= 0; i < this.redc; i++)
			reader[i]= new RecordReader(reducewriter.get(i));
		
		i= 0;
		while(in.hasRecordsLeft())
		{
			String[] h= in.readRecord();
			int f= Integer.parseInt(h[1]);
			List<String[]> recs= reader[f].readSession();
			
			// Records can be empty when no data was written
			if( recs != null )
			{
				for(String[] rec : recs)
				{
					if( rec[0] == null && rec[1] == null ) {}
					else
					{
						System.out.println("Write: (" + h[0] + "/" + rec[0] + "): " + rec[1] + " " + i + " " + f);
						out.write(rec[0], rec[1]);
					}
				}
			}
			i++;
		}

		out.close();
		for(i= 0; i < this.redc; i++)
			reader[i].close();
	}
}
