package at.dobiasch.mapreduce.framework.controller;

import java.io.FileWriter;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import org.nlogo.headless.HeadlessWorkspace;
import org.nlogo.nvm.Workspace;

import at.dobiasch.mapreduce.framework.ChecksumHelper;
import at.dobiasch.mapreduce.framework.SysFileHandler;
import at.dobiasch.mapreduce.framework.TaskType;
import at.dobiasch.mapreduce.framework.partition.HashPartitioner;
import at.dobiasch.mapreduce.framework.partition.IPartitioner;
import at.dobiasch.mapreduce.framework.task.IntKeyVal;

public class HostTaskController
{
	// List of Map Tasks
	// List of Keys to be reduced
	Map<Workspace,Data> maptasks;
	Map<String,IntKeyVal> intdata;
	SysFileHandler sysfileh;
	
	private Object syncMap;
	private boolean syncMapwait;
	private boolean syncIntwait;
	private Object syncInt;
	private FileWriter[] reduceout;
	private IPartitioner part;
	
	public HostTaskController(SysFileHandler sysfileh)
	{
		maptasks= new HashMap<Workspace,Data>();
		intdata= new HashMap<String,IntKeyVal>();
		this.sysfileh= sysfileh;
		syncIntwait= syncMapwait= false;
		syncMap= new Object();
		syncInt= new Object();
		this.part= new HashPartitioner();
	}
	
	public void addMap(HeadlessWorkspace ws, long ID, String key, long start, long end)
	{
		Data data= new Data(ID, TaskType.Map, key, key, start, end);
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
			maptasks.put(ws, data);
			// System.out.println("After add " + map);
			syncMapwait= false;
			syncMap.notifyAll();
		}
	}
	
	public void removeMap(HeadlessWorkspace ws)
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
			maptasks.remove(ws);
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
			ret= maptasks.get(ws);
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
			data= maptasks.get(ws);
			syncMapwait= false;
			syncMap.notifyAll();
		}
		if( data.type == TaskType.Map ) // emmited from a Map Task
		{
			// System.out.println("was map");
			IntKeyVal h;
			synchronized( syncInt ) // get Intermediate-Data access for the key 
			{
				while( syncIntwait )
				{
					try
					{
						syncInt.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				syncIntwait= true;
				
				h= intdata.get(key);
				
				syncIntwait= false;
				syncInt.notifyAll();
			}
			
			if( h == null ) // First value for this key
			{
				// System.out.println("First for " + key);
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
				h= new IntKeyVal(fn);
				
				synchronized( syncInt )
				{
					while( syncIntwait )
					{
						try
						{
							syncInt.wait();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					syncIntwait= true;
					
					intdata.put(key, h);
					
					syncIntwait= false;
					syncInt.notifyAll();
				}
				
			}
			
			h.writeValue(value);
			
			// System.out.println("written");
		}
		else // emmited from an reducer
		{
			// System.out.println("Emit " + key + " " + value);
			// System.out.println("Emit '" + key + "'");
			data.dest.write(key + ": " + value + "\n");
		}
	}
	
	/**
	 * Close all opened Files
	 * @throws IOException 
	 */
	public void closeIntermediateFiles() throws IOException
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
		return intdata;
	}
	
	public void setReduceOutput(FileWriter[] out)
	{
		this.reduceout= out;
	}

	public void addReduce(HeadlessWorkspace ws, long ID, String key, String filename, 
			long size)
	{
		FileWriter outf;
		outf= this.reduceout[part.getPartition(key,null,4)];
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
			maptasks.put(ws, data);
			// System.out.println("After add " + map);
			syncMapwait= false;
			syncMap.notifyAll();
		}
	}

	public void removeReduce(HeadlessWorkspace ws)
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
			maptasks.remove(ws);
			syncMapwait= false;
			syncMap.notifyAll();
		}
	}
}
