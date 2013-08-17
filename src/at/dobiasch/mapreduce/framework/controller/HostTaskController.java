package at.dobiasch.mapreduce.framework.controller;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.nlogo.workspace.AbstractWorkspace;

import at.dobiasch.mapreduce.framework.ChecksumHelper;
import at.dobiasch.mapreduce.framework.RecordReader;
import at.dobiasch.mapreduce.framework.RecordWriter;
import at.dobiasch.mapreduce.framework.RecordWriterBuffer;
import at.dobiasch.mapreduce.framework.SysFileHandler;
import at.dobiasch.mapreduce.framework.TaskType;
import at.dobiasch.mapreduce.framework.task.IntKeyVal;

public class HostTaskController
{
	// List of Map Tasks
	// List of Keys to be reduced
	private class TaskManager
	{
		Map<AbstractWorkspace,Data> tasks;
		private Object syncMap;
		private boolean syncMapwait;
		
		public TaskManager()
		{
			tasks= new HashMap<AbstractWorkspace,Data>();
			syncMap= new Object();
			syncMapwait= false;
		}

		public void addTaskByWorkspace(AbstractWorkspace ws, Data data)
		{
			// System.out.println("TM: " + ws + " " + data);
			synchronized( syncMap )
			{
				while( syncMapwait )
				{
					try
					{
						syncMap.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				syncMapwait= true;
				// System.out.println("Adding Map for " + ws + " " + this);
				tasks.put(ws, data);
				// System.out.println("After add " + map);
				syncMapwait= false;
				syncMap.notifyAll();
			}
		}

		public Data getTaskDataByWorkspace(AbstractWorkspace ws)
		{
			Data data= null;
			
			synchronized( syncMap )
			{
				while( syncMapwait )
				{
					try
					{
						syncMap.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				syncMapwait= true;
				data= tasks.get(ws);
				syncMapwait= false;
				syncMap.notifyAll();
			}
			
			return data;
		}

		public void removeTaskByWorkspace(AbstractWorkspace ws)
		{
			synchronized( syncMap )
			{
				while( syncMapwait )
				{
					try
					{
						syncMap.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				syncMapwait= true;
				tasks.remove(ws);
				syncMapwait= false;
				syncMap.notifyAll();
			}
		}
	}
	
	
	TaskManager tasks;
	Map<String,IntKeyVal> intdata;
	SysFileHandler sysfileh;
	
	private ConcurrentHashMap<Integer,RecordWriter> reduceout;
	private RecordWriterBuffer mapout;
	// private IPartitioner part;
	
	public HostTaskController(SysFileHandler sysfileh)
	{
		tasks= new TaskManager();
		intdata= new HashMap<String,IntKeyVal>();
		this.sysfileh= sysfileh;
		// this.part= new HashPartitioner();
	}
	
	public void addMap(AbstractWorkspace ws, long ID, String key, long start, long end) throws InterruptedException
	{
		// Data data= new Data(ID, TaskType.Map, key, key, start, end);
		RecordWriter outf= this.mapout.get();
		outf.startSession(ID);
		Data data= new Data(ID, TaskType.Map, key, key, start, end, outf);
		System.out.println("HTC:addMap" + data);
		tasks.addTaskByWorkspace(ws, data);
	}
	
	public void removeMap(AbstractWorkspace ws, boolean success) throws IOException
	{
		Data data= tasks.getTaskDataByWorkspace(ws);
		if( success == false )
		{
			System.out.println("Maping failed: " + data.ID);
			data.dest.removeSession();
		}
		else
			data.dest.endSession();
		this.mapout.put(data.dest);
		tasks.removeTaskByWorkspace(ws);
	}

	public Data getData(AbstractWorkspace ws)
	{
		// System.out.println("Getting data for " + ws);
		// System.out.println(" " + this);
		// System.out.println(map);
		Data ret= tasks.getTaskDataByWorkspace(ws);
		return ret;
	}
	
	/**
	 * This method is called when a Tasks emits a value
	 * @param ws The workspace from which the (key,value) is emmited
	 * @param key
	 * @param value
	 * @throws IOException 
	 */
	public void emit(AbstractWorkspace ws, String key, String value) throws IOException
	{
		// System.out.println("emit <" + key + "," + value + ">");
		Data data;
		data= tasks.getTaskDataByWorkspace(ws);
		
		// System.out.println("emit " + data.ID + " <" + key + "," + value + ">" + data.src + data.start);	
		if( data.type == TaskType.Map ) // emmited from a Map Task
		{
			data.dest.write(key, value);
		}			
		else // emmited from an reducer
		{
			data.dest.write(key, value);
		}
	}
	
	/**
	 * Close all opened Files
	 * @throws IOException 
	 */
	private void closeIntermediateFiles() throws IOException
	{
		System.out.println("close intermediate files");
		for(IntKeyVal h : intdata.values())
		{
			// System.out.println("Closing " + h.fn);
			h.close();
		}
		System.out.println("closed");
	}
	
	public Map<String,IntKeyVal> getIntermediateData()
	{
		// TODO: make this parallel
		while(this.mapout.hasFiles() )
		{
			try {
				RecordWriter w= this.mapout.get();
				RecordReader r= new RecordReader(w);
				System.out.println(w.getFilename());
				while(r.hasRecordsLeft())
				{
					String[] rec= r.readRecord();
					
					IntKeyVal h;
					String key;
					
					key= rec[0];
					if( key == null )
						key= "";
                    h= intdata.get(key);
                    // System.out.println(w.getFilename());
                    // System.out.println(r.recs);
                    // System.out.println(rec[0] + " " + rec[1]);
                    
                    if( h == null )
                    {
						String fn;
	                    MessageDigest md= null;
	                    try {
	                            md = MessageDigest.getInstance("SHA1");
	                    } catch (NoSuchAlgorithmException e) {
	                            e.printStackTrace();
	                    }
	                    md.update(key.getBytes());
	                    fn= sysfileh.addFile(ChecksumHelper.convToHex(md.digest()) + ".int");
	                    // System.out.println("New Key: " + key + fn);
	                    h= new IntKeyVal(key,fn);
	                    intdata.put(key, h);
                    }
                    
                    // System.out.println("Write " + key + " to " + h.fn);
                    h.writeValue(rec[1]);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		try {
			this.closeIntermediateFiles();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return intdata;
	}
	
	public void setReduceOutput(ConcurrentHashMap<Integer,RecordWriter> out)
	{
		this.reduceout= out;
	}

	public void addReduce(AbstractWorkspace ws, long ID, String key, String filename, 
			long size, int partitionNr) throws InterruptedException
	{
		// Here it is not necessary to use Partitioning!
		// FileWriter outf;
		// outf= this.reduceout[part.getPartition(key,null,4)];
		RecordWriter outf= this.reduceout.get(partitionNr);
		outf.startSession(ID);
		Data data= new Data(ID, TaskType.Reduce, filename, key, outf, size);
		
		tasks.addTaskByWorkspace(ws, data);
	}

	public void removeReduce(AbstractWorkspace ws, boolean success) throws IOException
	{
		Data data;
		data= tasks.getTaskDataByWorkspace(ws);
		if( success == false)
		{
			System.out.println("Reducer failed: " + data.ID);
			data.dest.removeSession();
		}
		else
			data.dest.endSession();
		// Not necessary any more since no buffer is used any more
		// -- Martin 2012/10/16 
		// this.reduceout.put(data.dest);
		tasks.removeTaskByWorkspace(ws);
	}

	public void setMapperOutput(RecordWriterBuffer mapwriter)
	{
		this.mapout= mapwriter;
	}
}
