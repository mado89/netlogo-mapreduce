package at.dobiasch.mapreduce.framework.controller;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import org.nlogo.headless.HeadlessWorkspace;
import org.nlogo.nvm.Workspace;

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
	Map<Workspace,Data> tasks;
	Map<String,IntKeyVal> intdata;
	SysFileHandler sysfileh;
	
	private Object syncMap;
	private boolean syncMapwait;
	private RecordWriterBuffer reduceout;
	private RecordWriterBuffer mapout;
	// private IPartitioner part;
	
	public HostTaskController(SysFileHandler sysfileh)
	{
		tasks= new HashMap<Workspace,Data>();
		intdata= new HashMap<String,IntKeyVal>();
		this.sysfileh= sysfileh;
		syncMap= new Object();
		// this.part= new HashPartitioner();
	}
	
	public void addMap(HeadlessWorkspace ws, long ID, String key, long start, long end) throws InterruptedException
	{
		// Data data= new Data(ID, TaskType.Map, key, key, start, end);
		RecordWriter outf= this.mapout.get();
		outf.startSession(ID);
		Data data= new Data(ID, TaskType.Map, key, key, start, end, outf);
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
	
	public void removeMap(HeadlessWorkspace ws, boolean success) throws IOException
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
			Data data= tasks.get(ws);
			if( success == false )
			{
				System.out.println("Maping failed: " + data.ID);
				data.dest.removeSession();
			}
			else
				data.dest.endSession();
			this.mapout.put(data.dest);
			tasks.remove(ws);
			syncMapwait= false;
			syncMap.notifyAll();
		}
	}

	public Data getData(Workspace ws)
	{
		// System.out.println("Getting data for " + ws);
		// System.out.println(" " + this);
		// System.out.println(map);
		Data ret;
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
			ret= tasks.get(ws);
			syncMapwait= false;
			syncMap.notifyAll();
		}
		return ret;
	}
	
	/**
	 * This method is called when a Tasks emits a value
	 * @param ws The workspace from which the (key,value) is emmited
	 * @param key
	 * @param value
	 * @throws IOException 
	 */
	public void emit(Workspace ws, String key, String value) throws IOException
	{
		// System.out.println("emit <" + key + "," + value + ">");
		Data data;
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
			// maptask= maptasks.keySet().contains(ws);
			data= tasks.get(ws);
			
			syncMapwait= false;
			syncMap.notifyAll();
		}
			
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
	                    // System.out.println(fn);
	                    h= new IntKeyVal(key,fn);
	                    intdata.put(key, h);
                    }
                    
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
	
	public void setReduceOutput(RecordWriterBuffer out)
	{
		this.reduceout= out;
	}

	public void addReduce(HeadlessWorkspace ws, long ID, String key, String filename, 
			long size) throws InterruptedException
	{
		// Here it is not necessary to use Partitioning!
		// FileWriter outf;
		// outf= this.reduceout[part.getPartition(key,null,4)];
		RecordWriter outf= this.reduceout.get();
		outf.startSession(ID);
		Data data= new Data(ID, TaskType.Reduce, filename, key, outf, size);
		
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

	public void removeReduce(HeadlessWorkspace ws, boolean success) throws IOException
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
			Data data= tasks.get(ws);
			if( success == false)
			{
				System.out.println("Reducer failed: " + data.ID);
				data.dest.removeSession();
			}
			else
				data.dest.endSession();
			this.reduceout.put(data.dest);
			tasks.remove(ws);
			syncMapwait= false;
			syncMap.notifyAll();
		}
	}

	public void setMapperOutput(RecordWriterBuffer mapwriter)
	{
		this.mapout= mapwriter;
	}
}