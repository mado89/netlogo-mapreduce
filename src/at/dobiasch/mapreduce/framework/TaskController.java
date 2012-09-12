package at.dobiasch.mapreduce.framework;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import org.nlogo.headless.HeadlessWorkspace;
import org.nlogo.nvm.Workspace;

public class TaskController
{
	public class Data
	{
		public long ID;
		public String src;
		public long start;
		public long end;
		
		public Data(long iD, String src, long start, long end)
		{
			ID = iD;
			this.src = src;
			this.start = start;
			this.end = end;
		}
	}
	
	/**
	 * Helper-Datastructure for intermediate Key-value pairs
	 */
	private class IntKeyVal
	{
		/**
		 * Filename
		 */
		public String fn;
		/**
		 * The output file to write out the values
		 */
		public BufferedWriter out;
		
		public IntKeyVal(String fn) throws IOException
		{
			this.fn= fn;
			out= new BufferedWriter(new FileWriter(new File(fn)));
		}
		
	}
	
	// List of Map Tasks
	// List of Keys to be reduced
	Map<Workspace,Data> maptasks;
	Map<String,IntKeyVal> intdata;
	String sysdir;
	
	public TaskController(String sysdir)
	{
		maptasks= new HashMap<Workspace,Data>();
		intdata= new HashMap<String,IntKeyVal>();
		this.sysdir= sysdir;
	}
	
	public void addMap(HeadlessWorkspace ws, long ID, String src, long start, long end)
	{
		// System.out.println("Adding Map for " + ws + " " + this);
		Data data= new Data(ID, src, start, end);
		maptasks.put(ws, data);
		// System.out.println("After add " + map);
	}
	
	public void removeMap(HeadlessWorkspace ws)
	{
		maptasks.remove(ws);
	}

	public Data getData(Workspace ws)
	{
		// System.out.println("Getting data for " + ws);
		// System.out.println(" " + this);
		// System.out.println(map);
		return maptasks.get(ws);
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
		if( maptasks.keySet().contains(ws) ) // emmited from a Map Task
		{
			IntKeyVal h= intdata.get(key);
			if( h == null ) // First value for this key
			{
				String fn;
				MessageDigest md= null;
				try {
					md = MessageDigest.getInstance("SHA1");
				} catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
				md.update(key.getBytes());
				fn= sysdir + "/" + ChecksumHelper.convToHex(md.digest());
				h= new IntKeyVal(fn);
			}
			h.out.write(value + "\n");
		}
		else // emmited from an reducer
		{
			
		}
	}
	
	/**
	 * Close all opened Files
	 * @throws IOException 
	 */
	public void closeIntermediateFiles() throws IOException
	{
		for(IntKeyVal h : intdata.values())
		{
			h.out.close();
		}
	}
}
